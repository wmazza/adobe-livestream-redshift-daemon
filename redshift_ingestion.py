import psycopg2
import boto3
import sys, os, json

key = sys.argv[1]
dir_path = os.path.dirname(os.path.realpath(__file__)) + '/'
producer_parameters_file = dir_path +  'data/producer_parameters.json'

def ingestionFromS3(key):
    with open(producer_parameters_file, 'r') as file:
        f = json.load(file)

        # S3 parameters
        s3_resource = boto3.resource('s3')
        bucket_name = f['S3_bucket_name']
        region_name = f['S3_region_name']
        livestream_bucket = s3_resource.Bucket(bucket_name)

        # Redshift parameters
        Redshift_staging_table_name = f['Redshift_staging_table_name']
        Redshift_columns = f['Redshift_columns']
        dbname = f['Redshift_dbname']
        host = f['Redshift_host']
        port = f['Redshift_port']
        user = f['Redshift_user']
        password = f['Redshift_password']

        IAM_Role = f['IAM_Role']

    # Setting connection
    redshift_connection = psycopg2.connect(
        dbname = dbname,
        host = host,
        port = port,
        user = user,
        password = password)
    cursor = redshift_connection.cursor()

    result = cursor.execute("COPY " + Redshift_staging_table_name + "(" + Redshift_columns + ") \
                FROM 's3://" + bucket_name + "/" + key + "' \
                iam_role '" + IAM_Role + "' \
                region '" + region_name + "' \
                EMPTYASNULL \
                delimiter '~';")

    cursor.close()
    redshift_connection.commit()
    redshift_connection.close()

if __name__ == "__main__":

    ingestionFromS3(key)