import json
import datetime
import dateutil.parser
import os

dir_path = os.path.dirname(os.path.realpath(__file__)) + '/'
flag_save_record = False

def saveRecord(record: dict):
    s = json.dumps(record, indent=4, sort_keys=True)
    with open(dir_path + "data/records_with_issues_TRANSFORM.json", 'a', buffering=20*(1024**2)) as myfile:
        myfile.write(s)

def timestampFormatting(timestamp_value: str) -> str:
    try:
        return str(dateutil.parser.parse(timestamp_value))
    except dateutil.parser.ParserError as pe:
        if(len(timestamp_value) >= 26 and ' 0 ' in timestamp_value):
            return str(dateutil.parser.parse(timestamp_value.replace(' 0 ', ' ')))
        else:
            #global flag_save_record
            #flag_save_record = True
            return str(dateutil.parser.parse('1970-01-01 00:00:00'))

def convertTimestamp(timestamp_value: str) -> str:

    return datetime.datetime.fromtimestamp(int(timestamp_value)).strftime('%Y-%m-%d %H:%M:%S')

def getEvent(eventValue: str) -> str:
    if(eventValue != None):
        return eventValue[0].get('count')
    else:
        return ''
        
def replaceDelimiter(string_value: str) -> str:
    return string_value.replace('~','') 

def checkNumeric(string_value: str) -> str:
    if(string_value.isnumeric()):
        return string_value
    else:
        return ''

def numericToInteger(string_value: str) -> str:
    if(string_value.replace('.', '').isnumeric()):
        return int(float(string_value))
    else:
        return ''

def transform(response_json: dict, batch_size: int, copy_approach: str):

    # TRANSFORMATION AND MANIPULATION OF JSON RECORDS TO FORMAT AS CSV
    try:
        transformed_response_csv = ''
        
		#global flag_save_record
        #if(flag_save_record):
        #    saveRecord(response_json)
        #    flag_save_record = False

        return transformed_response_csv

    except Exception as e:
        saveRecord(response_json)
        return "EXCEPTION OCCURRED"

    return None

		
		