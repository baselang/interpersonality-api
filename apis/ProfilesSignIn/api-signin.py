"""API Module to make a user signin.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- Getting user cridentials i.e. email and password of a user
- checking users validity
- Returning the JSON response with message, authorization token and success status code

"""

import jwt
import json
import pymysql
import logging
import traceback
from os import environ
from datetime import datetime  
from datetime import timedelta
import configparser


message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('signin.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# Getting the database Secret key and secret key for getting token
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')
key = environ.get('DB_ENCRYPTION_KEY')
TOKEN_EXPIRY_TIME = environ.get('TOKEN_EXPIRY_TIME')
TOKEN_EXP_TIME_EMAIL_USER = environ.get('TOKEN_EXP_TIME_EMAIL_USER')

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
    logger.error(errmsg)
    return  {
                "statusCode": status_code,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def handler(event,context):
    """Function to handle the request for Sign In API."""
    global message_by_language
    global TOKEN_EXP_TIME_EMAIL_USER
    global TOKEN_EXPIRY_TIME
    try:
        logger.info(event)
        # checking that the following event call is from lambda warmer or not
        if event['source']=="lambda_warmer":
            logger.info("lambda warmed")
            # returning the success json
            return {
                       'status_code':200,
                       'body':{"message":"lambda warmed"}
                   }
    except:
        # If there is any error in above operations
        pass
        
    try:
        # Fetching data from event
        data = json.loads(event['body'])
        email = data['email'].lower()
        password = data['password']
        language_id = event['headers']['language_id']
        message_by_language = str(language_id) + "_MESSAGES"
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        try:
            # Constructing the query to get user which has matching email and password
            selectionQuery = "SELECT u.`id`,cast(AES_DECRYPT(u.`name`, %s) as char),u.`language_id`,u.`user_id`,u.`social_userid` FROM `users` u INNER JOIN `user_emails` e ON u.`id` = e.`rid` and cast(AES_DECRYPT(e.`email`, %s) as char)=%s and BINARY cast(AES_DECRYPT(u.`password`, %s) as char)=%s"
            cursor.execute(selectionQuery, (key, key, email, key, password))

            result_list = []
            for result in cursor: result_list.append(result)

            if len(result_list) == 0:
                # logging the error when the entered email or password is invalid
                return log_err(config[message_by_language]['USER_STATUS'], 400)

            if result_list[0][4] == None :
                expiry_time = int(TOKEN_EXP_TIME_EMAIL_USER)
            else :
                expiry_time = int(TOKEN_EXPIRY_TIME)

            payload = {}
            payload['id'] = result_list[0][0]
            payload['name'] = result_list[0][1]
            payload['user_id'] = result_list[0][3]
            payload['language_id'] = result_list[0][2]
            payload['exp'] = datetime.timestamp(datetime.now() + timedelta(days=expiry_time))
            message_by_language = str(result_list[0][2]) + "_MESSAGES"
            try:
                token =  jwt.encode(payload, SECRET_KEY)
                logger.debug("token: " + str(token)[2:-1])
            except:
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['TOKEN_STATUS'], 500)
            # Returning JSON response
            return {
                        'statusCode': 200,
                        'headers':
                        {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                        },
                        'body': json.dumps({'auth':token.decode('utf-8'), 'user_id':result_list[0][3], 'rid':result_list[0][0], 'language_id':result_list[0][2]})
                    }

        except:
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
    finally:
        try:
            # Finally, clean up the connection
            cursor.close()
            cnx.close()
        except:
            pass
        
if __name__== "__main__":
    handler(None,None)