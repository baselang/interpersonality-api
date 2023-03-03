"""API Module to update the answers of questions for user in our application.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching all the terms and condition related field from user
- Update of all the user responses of new user into the old user
- Returning the JSON response with success status code with the message ,authentication token and user_id, language_id in the response body

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
config.read('update_users_answers.properties', encoding = "ISO-8859-1")

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

def log_err(errmsg , status_code):
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
    """Function to update the answers of questions for user"""
    global message_by_language
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
        # If there is any error in above operations, logging the error
        pass
    try:
        # Fetching data from event and rendering it
        body = json.loads(event['body'])
        selected_option = body['option']
        auth_token = event['headers']['Authorization']
        user_id = event['headers']['user_id']
        id = event['headers']['rid']

    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
        
    try:
        # verifying that the user is authorized or not to see this api's data
        old_id, old_userid, language_id = jwt_verify(auth_token)
    except:
        # if user does not have valid authorization
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['UNAUTHORIZED'], 403)
        
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
        
    try:
        # Query for getting current language of the user
        selectionQuery = "SELECT `language_id` FROM `users` WHERE `id`=%s"
        # Executing the Query
        cursor.execute(selectionQuery, (id))
        
        result_list = []
        # fetching result from the cursor
        for result in cursor: result_list.append(result)
        
        # getting current language_id of the user 
        language_id = result_list[0][0]
        message_by_language = str(language_id) + "_MESSAGES"
        
        # Query for getting all the terms and condition related field from user_permissions table
        selectionQuery = "SELECT `is_customized`,`is_ads`,`is_email` FROM `user_permissions` WHERE `rid`=%s"
        # Executing the Query
        cursor.execute(selectionQuery, (id))
        
        result_list = []
        # fetching result from the cursor
        for result in cursor: result_list.append(result)
        
        # Query for updating terms and condition
        updationQuery = "UPDATE `user_permissions` SET `is_customized`=%s,`is_ads`=%s,`is_email`=%s WHERE `rid`=%s"
        # Executing the Query
        cursor.execute(updationQuery, (result_list[0][0], result_list[0][1], result_list[0][2], old_id))
    except:
        # If there is any error in above operations, logging the error
        return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
        
    if selected_option == 'old':
        try:
            deletionQuery = "DELETE FROM `users` WHERE `id`=%s"
            cursor.execute(deletionQuery, (id))
        except:
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['DELETE_USERS'], 500)
    elif selected_option == 'new':
        # deletion queries for deleting all previous user_responses data and the result generated from that data
        try:
            deletionQuery = "DELETE FROM `user_responses` WHERE `rid`=%s"
            cursor.execute(deletionQuery, (old_id))
            
            deletionQuery = "DELETE FROM `output_responses` WHERE `rid`=%s"
            cursor.execute(deletionQuery, (old_id))
            
            deletionQuery = "DELETE FROM `user_profile_report` WHERE `rid`=%s"
            cursor.execute(deletionQuery, (old_id))
            
            deletionQuery = "DELETE FROM `user_theme_style` WHERE `rid`=%s"
            cursor.execute(deletionQuery, (old_id))
            
            deletionQuery = "DELETE FROM `user_input_variables_30` WHERE `rid`=%s"
            cursor.execute(deletionQuery, (old_id))
        except:
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['DELETE_DATA'], 500)
            
        # Updation of all the user responses of new user into the old user
        
        try:
            updationQuery = "UPDATE `user_responses` SET `rid`=%s, `user_id`=%s WHERE `rid`=%s"
            cursor.execute(updationQuery, (old_id, old_userid, id))
            
            updationQuery = "UPDATE `output_responses` SET `rid`=%s, `user_id`=%s WHERE `rid`=%s"
            cursor.execute(updationQuery, (old_id, old_userid, id))
            
            updationQuery = "UPDATE `user_input_variables_30` SET `rid`=%s, `user_id`=%s WHERE `rid`=%s"
            cursor.execute(updationQuery, (old_id, old_userid, id))
            
            updationQuery = "UPDATE `user_emails` SET `rid`=%s, `user_id`=%s WHERE `rid`=%s"
            cursor.execute(updationQuery, (old_id, old_userid, id))
        except:
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['UPDATE_DATA'], 500)
            
        try:
            deletionQuery = "DELETE FROM `users` WHERE `id`=%s"
            cursor.execute(deletionQuery, (id))
        except:
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['DELETE_DATA'], 500)
        
    return {
                'statusCode': 200,
                'headers':{
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                    },
                
                'body': json.dumps({"auth":auth_token, "user_id":old_userid, "language_id":language_id, "rid":old_id})
            }

if __name__== "__main__":
    handler(None,None)