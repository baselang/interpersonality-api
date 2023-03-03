"""API Module to validate resetlink url

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- Fetching the token
- validating and authenticating the token passed in resetlinkurl
- Returning the JSON response with success status code or with particular error message

"""

import json
import pymysql
import logging
import traceback
from os import environ
from datetime import datetime
from datetime import timedelta
from pyDes import *
import configparser

message_by_language = "165_MESSAGES"

# reading values from property file
config = configparser.ConfigParser()
config.read('validatelink.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')
key = environ.get('DB_ENCRYPTION_KEY')

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

def log_err(errmsg, statusCode):
    """Function to log the error messages."""
    return  {
                "statusCode": statusCode,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def handler(event,context):
    """Function to handle the request for ChangePasswordByEmailIntermediate."""
    global message_by_language
    logger.info(event)
    logger.info(event['headers']['language_id'])
    try:
        # Fetching data from event body
        my_data = event['headers']['Authorization']
        language_id = event['headers']['language_id']
        message_by_language = str(language_id) + "_MESSAGES"
    except:
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
        
    # decrypting the encrypted data
    try:
        ciphertext = bytes.fromhex(my_data)
        plain_text = (triple_des(key).decrypt(ciphertext, padmode=2)).decode('utf-8')
    except:
        return log_err (config[message_by_language]['INVALID_USER'], 500)
    
    # extracting information from the data found after decryption
    try:
        data = json.loads(plain_text)
        exp = data['exp']
        id = data['id']
        # checking that the token is expired or not
        if exp<datetime.timestamp(datetime.now()):
            return log_err (config[message_by_language]['LINK_EXPIRED'], 400)
    except:
        return log_err (config[message_by_language]['INVALID_TOKEN'], 500)
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        try:
            checkQuery = "SELECT `token`, `isActiveToken` FROM `users` WHERE `id`=%s"
            cursor.execute(checkQuery, (id))
            
            # Taking out id from cursor if user with given email id exist
            result_list = []
            for result in cursor: result_list.append(result)
            if (result_list[0][1]==0):
                return log_err(config[message_by_language]['LINK_LIMIT_REACHED'], 400)
            elif (result_list[0][0] != my_data):
                return log_err(config[message_by_language]['LINK_RENEWED'], 400)
        except:
            return log_err(config[message_by_language]['LINK_INVALID'], 500)
            
        return {
                    'statusCode': 200,
                    'headers':
                        {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                        },
                    'body': json.dumps({"message":config[message_by_language]['SUCCESS_MESSAGE']})
                }
    except:
        return log_err (config[message_by_language]['CONNECTION_CHECK'], 500)
    finally:
        try:
            # Finally, clean up the connection
            cursor.close()
            cnx.close()
        except:
            pass
        
if __name__== "__main__":
    handler(None,None)