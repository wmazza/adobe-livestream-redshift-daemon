import sys, time, datetime, os, subprocess, json
from daemon import Daemon
import livestream
import ls_transform_CSV
import authentication
import boto3
import uuid
import logging
from logging.handlers import TimedRotatingFileHandler


class livestream_producer_app(Daemon):

    ID = 0
    dir_path = os.path.dirname(os.path.realpath(__file__)) + '/'
    producer_parameters_file = dir_path +  'data/producer_parameters.json'
    
    def __init__(self, id):
        # Set the daemon ID (for multiple parallel daemon processes case)
        self.ID = id

        # Set the pid file
        pid = '/tmp/livestream_producer_app-' + str(id) + '.pid'

        # Set stdin, stdout and stderr 
        stdin = '/dev/null' #self.dir_path + 'data/logs/stdin.log' 
        stdout = '/dev/null' #self.dir_path + 'data/logs/stdout.log' 
        stderr = self.dir_path + 'data/logs/stderr.log' #'/dev/null'
        Daemon.__init__(self, pid, stdin, stdout, stderr)

        # Logging setup
        self.logger = logging.getLogger(__name__ + str(id))
        log_filename = 'data/logs/daemonlog_' + str(id)
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(filename)s - %(levelname)s - %(message)s") 
        handler = TimedRotatingFileHandler(self.dir_path + log_filename, when="midnight")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # Initalize variables from JSON file
        with open(self.producer_parameters_file, 'r') as file:
            f = json.load(file)
            self.maxConnections = f['maxConnections']
            self.reset = f['reset']
            self.smoothing = f['smoothing']
            self.smoothingBucketSize = f['smoothingBucketSize']
            self.BATCH_RECORDS = f['BATCH_RECORDS']
            self.BATCH_FILES = f['BATCH_FILES']
            self.copy_command_approach = f['copy_command_approach']
            self.S3_base_path = f['S3_base_path']

        self.logger.info('Init (ID): ' + str(id))

    def saveS3(self, object_body, key):
        with open(producer_parameters_file, 'r') as file:
            f = json.load(file)

            # S3 parameters
            s3_resource = boto3.resource('s3')
            bucket_name = f['S3_bucket_name']
            region_name = f['S3_region_name']
            livestream_bucket = s3_resource.Bucket(bucket_name)

        response = livestream_bucket.put_object(Body=object_body, Key=self.S3_base_path + key)
        self.logger.info('S3 put_object response: ' + str(response))


    #this is the overwritten method from the article by Sander Marechal
    # http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
    def run(self):
        # Initialize parameters
        datetime_now = datetime.datetime.now()
        timestamp_today = datetime_now.strftime('%Y%m%d')

        self.logger.info('Ingesting latest incomplete S3 directory for file not ingested before interruption.')
        with open(self.dir_path + 'data/s3_unique_id_directory.txt', 'r') as tfile:
            last_s3_unique_id = tfile.readline()

        # Upload temp file records to S3 to avoid possible missing records
        self.saveS3(open(self.dir_path + 'data/temp_csv_file.csv', 'rb'), self.S3_base_path + last_s3_unique_id)
        # Ingest data from directory on S3 populated prior to restart to avoid possible missing records
        ingestion_process = subprocess.Popen(['python3', self.dir_path + 'redshift_ingestion.py',  self.S3_base_path + last_s3_unique_id], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.logger.info('Ingestion process started: ' + str(ingestion_process) + ' - file_key: ' + self.S3_base_path + last_s3_unique_id)

        # Get ouput from process
        process_output, process_error = ingestion_process.communicate()
        self.logger.info('Result ' + str(ingestion_process) + ' - Output: ' + str(process_output.decode("utf-8") ) + ' - Error: ' + str(process_error.decode("utf-8") ))

        # Compute unique_id for directory on S3 and save to file to track last directory in case producer stops
        unique_id = timestamp_today + '/' + str(uuid.uuid4())
        with open(self.dir_path + 'data/s3_unique_id_directory.txt', "w") as tfile:
            tfile.write(unique_id)

        current_lines = 0
        current_files = 0
        authentication_message = ''
        object_body = ''

        while True:
            response = livestream.getLiveStream(self.maxConnections, self.reset, self.smoothing, self.smoothingBucketSize)
            self.logger.info('Starting stream parsing')                    
            for line in response.iter_lines():
                try:
                    # From bytes to string
                    line_string = line.decode('utf8')
                    # From string to json
                    if(len(line) > 0):
                        json_line = json.loads(line.decode('utf8'))

                        csv_transformed = ls_transform_CSV.transform(json_line, self.BATCH_RECORDS, self.copy_command_approach)

                        if(csv_transformed == "EXCEPTION OCCURRED"):
                            self.logger.exception('Exception occurred during record transformation to csv.') #Exception handling takes place in ls_transform_CSV.transform module
                            continue
                        
                        # Append new record to object_body string used for uploading to S3 
                        object_body += csv_transformed + '\n'
                        # Append to temp local file 
                        with open(self.dir_path + 'data/temp_csv_file.csv', "a") as temp_csv_file:
                            temp_csv_file.write(csv_transformed + '\n')
                        current_lines += 1

                except json.decoder.JSONDecodeError as e:
                    # Token to be refreshed
                    if(line_string == "not authorized"):
                        self.logger.warning("Token no more valid. Refreshing it.")
                        authentication_message =  authentication.authenticate()

                        break

                    else:
                        self.logger.error("EXCEPTION: " + str(e))
                        self.logger.warning('There was an issue with a record in string format; saving the record in data/records_with_issues_DECODER.json')
                        livestream.saveRecordString(line_string)

                        continue

                except Exception as e:
                    self.logger.error("EXCEPTION: " + str(e))
                    self.logger.warning('There was an issue with a record in json format; saving the record in data/records_with_issues_JSON.json')
                    livestream.saveRecordJSON(json_line)

                    continue

                if(current_lines == self.BATCH_RECORDS):
                    
                    key = unique_id + '/' + str(current_files) + '.csv'
                    self.saveS3(object_body, key)

                    open(self.dir_path + 'data/temp_csv_file.csv', 'w').close()
                    current_files += 1
                    current_lines = 0
                    object_body = ''

                if(current_files == self.BATCH_FILES):
                    # When we collect a total of BATCH_FILES number of files, a subprocess is called to run the actual ingestion throught he copy command
                    directory_key = self.S3_base_path + unique_id
                    ingestion_process = subprocess.Popen(['python3', self.dir_path + 'redshift_ingestion.py', directory_key], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    self.logger.info('Ingestion process started: ' + str(ingestion_process) + ' - file_key: ' + directory_key)
                    
                    # Get ouput from process
                    process_output, process_error = ingestion_process.communicate()
                    self.logger.info('Result ' + str(ingestion_process) + ' - Output: ' + str(process_output.decode("utf-8") ) + ' - Error: ' + str(process_error.decode("utf-8") ))
                    
                    current_files = 0
                    datetime_now = datetime.datetime.now()
                    timestamp_today = datetime_now.strftime('%Y%m%d')
                    unique_id = timestamp_today + '/' + str(uuid.uuid4()) # New unique ID for S3 directory
                    with open(self.dir_path + 'data/s3_unique_id_directory.txt', "w") as tfile: #save it to file in case producer stops
                        tfile.write(unique_id)



            if(authentication_message != ''):
                self.logger.info('Token Refreshed. ' + authentication_message)
                authentication_message = ''