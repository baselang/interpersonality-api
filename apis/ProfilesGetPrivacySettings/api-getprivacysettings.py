#!/usr/bin/env python3

"""API Module to Get Privacy Settings Functionalities.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Get Privacy Settings 
- Returning the JSON response with success status code

"""
import jwt
import json
import pymysql
import logging
import traceback
from os import environ
import configparser

# For getting messages according to language of the user
message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('getprivacysettings.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for data encryption and security token
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
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
    """Function to handle the request for Update Privacy Settings API."""
    logger.info(event)
    global message_by_language
    try:
        # fetching language_id from the event data
        auth_token = event['headers']['Authorization']
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
        
    try:
        # verifying that the user is authorized or not to see this api's data
        rid, user_id, language_id = jwt_verify(auth_token)
    except:
        # if user does not have valid authorization
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['UNAUTHORIZED'], 403)
        
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        try:
            # Query for getting current language of the user
            selectionQuery = "SELECT `language_id`, `privacy_settings_status` FROM `users` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (rid))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            # getting current language_id of the user 
            language_id = result_list[0][0]
            privacy_settings_status = result_list[0][1]
            message_by_language = str(language_id) + "_MESSAGES"
            
            # Constructing the query to get the user permissions of the user
            query = "SELECT `is_customized`, `is_email`, `is_ads`, `is_privacy_consent` from `user_permissions` WHERE `rid`=%s"
            # Executing the query using cursor
            cursor.execute(query, (rid))
            
            permission_list = []
            # fetching permissions list from the cursor
            for result in cursor: permission_list.append(result)
            # fetching permissions from the permissions list
            is_customized = permission_list[0][0]
            is_email = permission_list[0][1]
            is_ads = permission_list[0][2]
            is_privacy_consent = permission_list[0][3]
            
            # returning the success json
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
                'body': json.dumps({'privacy_settings_status':privacy_settings_status, 'is_customized':is_customized, 'is_email':is_email, 'is_ads':is_ads, 'is_privacy_consent':is_privacy_consent})
            }
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)
    finally:
        try:
            # Finally, clean up the connection
            cursor.close()
            cnx.close()
        except: 
            pass

if __name__== "__main__":
    handler(None,None)
