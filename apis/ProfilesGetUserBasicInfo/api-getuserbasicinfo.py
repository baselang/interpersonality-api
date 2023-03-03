"""API Module to get the user basic information in our application.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching the user information from DB
- Returning the JSON response with success status code with the message ,authentication token and user related information in the response body

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
config.read('getuserbasicinfo.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')
delete_flag = int(environ.get('DELETE_FLAG'))

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
    """Function to to get the user basic information"""
    global message_by_language
    global delete_flag
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
        # Fetching data from event and rendering it
        auth_token = event['headers']['Authorization']
    except:
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
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
        
    try:
        # Constructing the query to get user basic info details
        selectionQuery = "SELECT CAST(AES_DECRYPT(`firstname`, %s) AS char),CAST(AES_DECRYPT(`lastname`, %s) AS char),CAST(AES_DECRYPT(`primary_email`, %s) AS char),`id`,`social_userid`,`language_id`,`picture_url`, `is_picture_uploaded`, `password` FROM `users` WHERE `id`=%s"
        # Executing the query
        cursor.execute(selectionQuery, (key, key, key, rid))
        result_list = []
        # Fetching result in cursor
        for result in cursor: result_list.append(result)
        # Fetching user info from the result from cursor
        firstname = result_list[0][0]
        lastname = result_list[0][1]
        primary_email = result_list[0][2]
        social_userid = result_list[0][4]
        rid = result_list[0][3]
        language_id = result_list[0][5]
        message_by_language = str(language_id) + "_MESSAGES"
        picture_url = result_list[0][6]
        is_picture_uploaded = result_list[0][7]
        password = result_list[0][8]
        
        # checking user is having password or not
        if password:
            is_password = True
        else:
            is_password = False
        # checking user is connected to facebook or not
        if social_userid:
            # if connected than sending true
            is_connected = True
        else:
            # if not connected sending false
            is_connected = False
            
        # Query for fetching language name according to provided language_id
        selectionQuery = "SELECT `name` FROM `language` WHERE `id`=%s"
        # Executing the query
        cursor.execute(selectionQuery, (language_id))
        language = []
        # Fetching result in cursor
        for result in cursor: language.append(result)
        # fetching language from above result
        language = language[0][0]
        
        # Query for getting emails of a particular user which are not primary email
        selectionQuery = "SELECT CAST(AES_DECRYPT(`email`, %s) AS char) FROM `user_emails` WHERE `rid`=%s AND `email`!=AES_ENCRYPT(%s, %s)"
        # Executing the query
        cursor.execute(selectionQuery, (key, rid, primary_email, key))
        emails = []
        # Fetching result in cursor which contain all the emails
        for result in cursor: emails.append({"email":result[0]})
        

        if picture_url == None and is_picture_uploaded == 0:
            delete_flag = 0

        elif picture_url != None and is_picture_uploaded == 0:
            delete_flag = 0

        elif picture_url !=None and is_picture_uploaded == 1:
            delete_flag = 1



        # returning the success json with the required data
        return {
                    'statusCode': 200,
                    'headers':{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                            },
                    'body': json.dumps({"auth":auth_token, "user_id":user_id, "firstname":firstname, "lastname":lastname, "primary_email":primary_email, "language_id":language_id, "language":language,"emails":emails, "picture_url":picture_url, "is_connected":is_connected, "delete_flag": delete_flag, "is_password":is_password})
                    }
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['QUERY_STATUS'], 500)
        
if __name__== "__main__":
    handler(None,None)