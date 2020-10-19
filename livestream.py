import requests
import json
import os

dir_path = os.path.dirname(os.path.realpath(__file__)) + '/'

header = {"Accept": "application/json",
           "Content-Type": "application/json",
           "Authorization": "Bearer ",
           "X-Api-Key": ""
           }
parameter = {"maxConnections": 1, # Indicates how many clients will connect to the same stream.	An integer between 1 and 8
             "reset": "largest" # Indicates age of data to return during initial connection/reconnection. smallest will stream the oldest possible data. largest will stream the newest possible data.
             #"smoothing": 1, # Smooths the rate of records returned by Livestream by using a server-side buffer. 1 to enable smoothing. Remove the parameter to disable it.
             #"smoothingBucketSize": 270 # The size of the time window to use to determine the average traffic rate that is used in smoothing data returned by Livestream. An integer between 1 and 7200. The default is 270 seconds.
            }
dir_path = os.path.dirname(os.path.realpath(__file__)) + '/'

def getLiveStream(number_connections: int, reset: str, smoothing: int, smoothingBucketSize: int):
   
    with open(dir_path + "data/token.json", "r") as token:
        json_token = json.load(token)
        access_token = json_token["access_token"]
        
    with open(dir_path + 'data/config_admin.json', 'r') as file:
            f = json.load(file)
            LiveStreamEndpoint = f['livestreamEndpoint']
            StreamEndpoint = f['AdobeStreamEndpoint']

    header["Authorization"] = "Bearer " + access_token
    
    # Define parameter values
    parameter["maxConnections"] = number_connections
    parameter["reset"] = reset
    if(smoothing == 1):
        parameter["smoothing"] = 1
        parameter["smoothingBucketSize"] = smoothingBucketSize    

    live_stream_response = requests.get(LiveStreamEndpoint + StreamEndpoint, headers=header, stream=True, params=parameter)

    return live_stream_response


def saveRecordJSON(record: dict):
    json_string = json.dumps(record, indent=4, sort_keys=True)

    with open(dir_path + "data/records_with_issues_JSON.json", 'a', buffering=20*(1024**2)) as record_json:  
        record_json.write(json_string)

def saveRecordString(record: str):
    with open(dir_path + "data/records_with_issues_DECODER.json", 'a', buffering=20*(1024**2)) as record_string:  
        record_string.write(record + "\n")
