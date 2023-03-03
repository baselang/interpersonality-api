"""API For updating the is_got_it flag to 1 to hide the Got it section

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching data from request
- Updating the is_got_it flag related to a user_partner_id
- Returning the JSON response with success status code and required data
"""

import jwt
import json
import pymysql
import logging
import traceback
from os import environ
import configparser

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('update_coaching_gotit.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

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
    language_id = int(payload['language_id'])
    return rid, user_id, language_id

def handler(event,context):
    """Function to handle the request for Getting the got it on coaching page API"""
    global message_by_language
    logger.info(event)
    try:
        # Fetching data from event and rendering it
        auth_token = event['headers']['Authorization']
        user_partner_id = int(event['headers']['user_partner_id'])
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
    
    try:
        # verifying that the user is authorized or not to see this api's data
        rid, user_id, language_id = jwt_verify(auth_token)
        message_by_language = str(language_id) + "_MESSAGES"
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
            selectionQuery = "SELECT `language_id` FROM `users` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (rid))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            # fetching the data from the cursor
            language_id = int(result_list[0][0])
            message_by_language = str(language_id) + "_MESSAGES"
            
            # Query for getting current language of the user
            selectionQuery = "SELECT `user_rid` FROM `user_partner_products` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (user_partner_id))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            # fetching the data from the cursor
            user_rid = int(result_list[0][0])
            
            if user_rid != rid:
                # If the user does not have permission to access these resource then sending the error
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['INVALID_ACCESS'], 500)
            
            # Query for updating is_got_it from database
            updationQuery = "UPDATE `user_partner_products` SET `is_got_it`=1 WHERE `id`=%s"
            # Executing the Query
            cursor.execute(updationQuery, (user_partner_id))
            
            # Query for getting is_got_it from database
            selectionQuery = "SELECT `is_got_it` FROM `user_partner_products` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (user_partner_id))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            # fetching the data from the cursor
            is_got_it = int(result_list[0][0])
            
            # returning success json with the required data
            return {
                        'statusCode': 200,
                        'headers':{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                            },
                        'body': json.dumps({"is_got_it": is_got_it})
                    }
        except:
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)

if __name__== "__main__":
    handler(None,None)