"""API Module to deleting Active Campaign Contact.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching data required for api
- deleting a contact in ActiveCampaign
- sending the success json with the required data i.e. all notifications

"""

import jwt
import json
import requests
import logging
import traceback
from os import environ
import configparser

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('delete_ac_contact.properties', encoding = "ISO-8859-1")

# Getting key for getting token
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

# required environment variables
AC_BASE_URL = environ.get('AC_BASE_URL')
DELETE_CONTACT_URL = environ.get('DELETE_CONTACT_URL')
API_TOKEN = environ.get('API_TOKEN')

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# getting message variable

message_by_language = "165_MESSAGES"

# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
# Setting the log level to INFO
logger.setLevel(logging_Level)

logger.info("Cold start complete.")

def make_connection():
    """Function to make the database connection."""
    return pymysql.connect(host=endpoint, user=dbuser, passwd=password,
        port=int(port), db=database, autocommit=True)

def log_err(errmsg, status_code):
    """Function to log the error messages."""
    logger.info(errmsg)
    return  {
                "statusCode": status_code,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }
            
def jwt_verify(auth_token):
    """Function to verify the authorization token"""
    # decoding the authorization token provided by user
    payload = jwt.decode(auth_token, SECRET_KEY, options={'require_exp': True})
    
    # setting the required values in return
    rid = int(payload['id'])
    user_id = payload['user_id']
    language_id = payload['language_id']
    return rid, user_id, language_id

def handler(event,context):
    """Function to handle the request for delete contact API"""
    global message_by_language
    logger.info("Event :::::::")
    logger.info(event)
    
    try:
        # getting data from the users request
        auth_token = event['headers']['Authorization']
        contact_id = event['headers']['contact_id']
    except:
        # if above code failed than returning the failure json and tracing the error
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
    
    try:
        # verifying that the user is authorized or not to see this api's data
        rid, user_id, language_id = jwt_verify(auth_token)
    except:
        # if user does not have valid authorization
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['UNAUTHORIZED'], 403)
    
    try:    
        # defining a params dict for the parameters to be sent to the API rid
        HEADER = {'Api-Token':API_TOKEN}
        
        # creating an actual url for deleting the contact from ActiveCampaign
        DELETE_CONTACT_AC_URL = (AC_BASE_URL + DELETE_CONTACT_URL).format(int(contact_id))
        
        logger.info("DELETE_CONTACT_URL ::::::::")
        logger.info(DELETE_CONTACT_AC_URL)
        
        # sending post request and saving response as response object 
        response = requests.delete(url = DELETE_CONTACT_AC_URL, headers = HEADER)
        
        # preparing success json  with result_list
        logger.info("status code ::::::::")
        logger.info(response.status_code)
        logger.info("Delete contact Details response ::::::::")
        logger.info(response.json())            
        logger.info('contact_id :::::::')
        logger.info(contact_id)
        
        if int(response.status_code) == 200:
            # returning success json
            return {
                        'statusCode': 200,
                        'headers':{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                        },
                        'body': json.dumps({"message": (config[message_by_language]['SUCCESS_STATUS']).format(int(contact_id))})
                    }
        else:
            # if the request is not successfully executed than raising the exception with the status
            response.raise_for_status()
            return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
    except:
        # if above code failed than returning the failure json and tracing the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
        
if __name__== "__main__":
    handler(None, None)

