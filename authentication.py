import jwt as _jwt
import requests as _requests
import json 
import time
import os

_org_id, _api_key, _tech_id, _pathToKey, _secret, _companyid, _TokenEndpoint, _LiveStreamEndpoint = "", "", "", "", "", "", "", ""
_orga_admin = {'_org_admin', '_deployment_admin', '_support_admin'}
_date_limit = 0
_token = ''
_header = {"Accept": "application/json",
           "Content-Type": "application/json",
           "Authorization": "Bearer ",
           "X-Api-Key": ""
           }
dir_path = os.path.dirname(os.path.realpath(__file__)) + '/'

def authenticate() -> str:
    importConfigFile(dir_path + 'data/config_admin.json')
    token_validity = retrieveToken(verbose=True, save=True)

    return token_validity


def importConfigFile(file: str) -> None:
    """
    This function will read the 'config_admin.json' to retrieve the information to be used by this module. 
    Arguments:
        file: REQUIRED : file (if in the same folder) or path to the file and file if in another folder.


    Example of file value.
    "config.json"
    "./config.json"
    """
    global _org_id
    global _api_key
    global _tech_id
    global _pathToKey
    global _secret
    global _header
    global _TokenEndpoint
    global _LiveStreamEndpoint
    if file.startswith('/'):
        file = "."+file
    with open(file, 'r') as file:
        f = json.load(file)
        _org_id = f['org_id']
        _api_key = f['api_key']
        _header["X-Api-Key"] = f['api_key']
        _tech_id = f['tech_id']
        _secret = f['secret']
        _pathToKey = dir_path + f['pathToKey'] 
        _TokenEndpoint = f['tokenEndpoint']
        _LiveStreamEndpoint = f['livestreamEndpoint']


def retrieveToken(verbose: bool = False, save: bool = False, **kwargs) -> str:
    """ Retrieve the token by using the information provided by the user during the import importConfigFile function. 

    Argument : 
        verbose : OPTIONAL : Default False. If set to True, print information.
        save : OPTIONAL : Default False. If set to True, will save the token in a json file (data/token.json). 
    """
    global _token
    global _header
    global _pathToKey

    # importConfigFile('data/config_admin.json')

    if _pathToKey.startswith('/'):
        _pathToKey = "."+_pathToKey
    with open(_pathToKey, 'r') as f:
        private_key_unencrypted = f.read()
        header_jwt = {'cache-control': 'no-cache',
                      'content-type': 'application/x-www-form-urlencoded'}
    jwtPayload = {
        # Expiration set to 24 hours
        "exp": round(24 * 60 * 60 + int(time.time())),
        "iss": _org_id,  # org_id
        "sub": _tech_id,  # technical_account_id
        "https://ims-na1.adobelogin.com/s/ent_analytics_bulk_ingest_sdk": True,
        "aud": "https://ims-na1.adobelogin.com/c/" + _api_key
    }
    encoded_jwt = _jwt.encode(jwtPayload, private_key_unencrypted, algorithm='RS256')  # working algorithm
    payload = {
        "client_id": _api_key,
        "client_secret": _secret,
        "jwt_token": encoded_jwt.decode("utf-8")
    }
    response = _requests.post(_TokenEndpoint, headers=header_jwt, data=payload)
    
    json_response = response.json()
    token = json_response['access_token']
    _header["Authorization"] = "Bearer "+token
    expire = json_response['expires_in']
    global _date_limit  # getting the scope right
    _date_limit = time.time() + expire / 1000 - 500  # end of time for the token
    if save:
        with open(dir_path + 'data/token.json', 'w') as f:  # save the token
            f.write('{"access_token" : "' + token + '"}')
        if verbose:
            return_message = 'Token valid until: ' + time.ctime(time.time() + expire / 1000)
    return return_message


def _checkToken(func):
    """decorator that checks that the token is valid before calling the API"""

    def checking(*args, **kwargs):  # if function is not wrapped, will fire
        global _date_limit
        now = time.time()
        if now > _date_limit - 1000:
            global _token
            _token = retrieveToken(*args, **kwargs)
            return func(*args, **kwargs)
        else:  # need to return the function for decorator to return something
            return func(*args, **kwargs)

    return checking  # return the function as object
