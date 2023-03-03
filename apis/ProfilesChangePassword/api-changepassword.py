"""API Module to change password of a user when he is Logged In.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- Getting authorization token and new password
- updating password with new password
- Returning the JSON response with message, authorization token and success status code

"""
import jwt
import json
import pymysql
import logging
import traceback
from os import environ
import configparser

# getting messages according to languages
message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('changepassword.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# Getting the database Secret key and secret key for getting token
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')
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

def log_err(errmsg):
    """Function to log the error messages."""
    logger.error(errmsg)
    return  {
                "statusCode": 500,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def handler(event,context):
    """Function to handle the request and provide change password functionality with response"""
    global message_by_language
    try:
        logger.info(event)
        # Fetching data from event
        data = json.loads(event['body'])
        
        # Fetching Authorization token and new password provided by user
        auth_token = event['headers']['Authorization']
        newpassword = data['newpassword']
    except:
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'])
        
    try:
        # Verifying access token is valid or not
        token_data = jwt.decode(auth_token.encode('utf-8'), SECRET_KEY,options={'require_exp': True})
        rid = token_data['id']
        language_id = token_data['language_id']
        message_by_language = str(language_id) + "_MESSAGES"
    except:
        return log_err (config[message_by_language]['TOKEN_STATUS'])
        
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
            
            # getting current language_id of the user 
            language_id = result_list[0][0]
            message_by_language = str(language_id) + "_MESSAGES"
            
            # Constructing the query update users password
            selectionQuery = "UPDATE `users` SET `password`=AES_ENCRYPT(%s, %s) WHERE `id`=%s"
            cursor.execute(selectionQuery, (newpassword, key ,rid))
            # Returning JSON response
            payload = {'auth':auth_token,'message':config[message_by_language]['SUCCESS_MESSAGE']}            
            return {
                    'statusCode': 200,
                    'headers':
                            {
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                            },
                    'body': json.dumps(payload)
                    }
        except:
            return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'])
    except:
        return log_err (config[message_by_language]['CONNECTION_STATUS'])
    finally:
        try:
            # Finally, clean up the connection
            cnx.close()
            cursor.close()
        except: 
            pass

if __name__== "__main__":
    handler(None,None)