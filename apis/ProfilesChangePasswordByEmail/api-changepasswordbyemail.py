"""API Module to change the password by using email link provided after forgot password process.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- Fetching the token and checking its validity and authenticity
- updating the password of the user after first step
- Returning the JSON response with success status code or with particular error message

"""
import jwt
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
config.read('changepasswordbyemail.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')
key = environ.get('DB_ENCRYPTION_KEY')

# Getting key for getting token
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
    """Function to handle the request for Get Big5 API."""
    global message_by_language
    logger.info(event)
    logger.info(event['headers']['language_id'])
    try:
        # Fetching data from event body
        data = json.loads(event['body'])
        new_password = data['newpassword']
        logger.info(new_password)
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
    except:
        return log_err (config[message_by_language]['INVALID_TOKEN'], 500)
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        try:
            # taking data of user to check that a users token is active or not and also other details
            checkQuery = "SELECT `token`, `isActiveToken`, `language_id`, `user_id`, CAST(AES_DECRYPT(`name`, %s ) AS CHAR) FROM `users` WHERE `id`=%s"
            cursor.execute(checkQuery, (key,id))
            
            # Taking out id from cursor if user with given email id exist
            result_list = []
            for result in cursor: result_list.append(result)
            if (result_list[0][1]==0):
                return log_err(config[message_by_language]['LINK_LIMIT_REACHED'], 500)
            elif (result_list[0][0] != my_data):
                return log_err(config[message_by_language]['LINK_RENEWED'], 500)
            language_id = result_list[0][2]
            user_id = result_list[0][3]
            name = result_list[0][4]
        except:
            return log_err(config[message_by_language]['LINK_INVALID'], 500)
            
        try:
            # Constructing the query to update password of a user
            selectionQuery = "UPDATE `users` SET `password`=AES_ENCRYPT(%s, %s),`isActiveToken`=0 WHERE `id`=%s"
            cursor.execute(selectionQuery, (new_password, key, id))
            
            # creating a payload for generating authentication token
            payload = {}
            payload['id'] = id
            payload['name'] = name
            payload['language_id'] = language_id
            payload['user_id'] = user_id
            payload['exp'] = datetime.timestamp(datetime.now() + timedelta(days=365))
            try:
                # generating an authentication token of a user
                token =  jwt.encode(payload, SECRET_KEY)
                logger.info("token: " + str(token)[2:-1])
            except:
                return log_err (config[message_by_language]['TOKEN_STATUS'], 500)
            
            # Returning JSON response           
            return {
                        'statusCode': 200,
                        'headers':
                            {
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                            },
                        'body': json.dumps({'auth':token.decode('utf-8'), 'user_id':user_id, 'is_account_already_exist':False, "message":config[message_by_language]['SUCCESS_MESSAGE'],"language_id":language_id})
                    }
        except:
            return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
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