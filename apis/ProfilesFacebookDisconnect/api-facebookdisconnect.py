"""API Module to make user disconnect with facebook in our application.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching facebook code from event
- Checking that user has any password associated with account or not because when he signup through facebook no password of user is needed it automatically signup using facebook 
- updating users to remove facebook related information of a user
- Returning the JSON response with success status code with the message ,authentication token and user_id in the response body

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
config.read('facebookdisconnect.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')
picture_upload_status = int(environ.get('PICTURE_UPLOAD_STATUS'))
is_fb_image_flag = environ.get('is_fb_image_flag')
# getting message variable

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
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
    """Function to handle the request for facebookdisconnect API"""
    try:
        # Fetching data from event and rendering it
        auth_token = event['headers']['Authorization']
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
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
        
    try:
        # Constructing the query to select email to check that user email exist or not
        selectionQuery = "SELECT CAST(AES_DECRYPT(`password`, %s) AS char),`id`,`social_userid`, `language_id`, `is_picture_uploaded` FROM `users` WHERE `id`=%s"
        # Executing the query
        cursor.execute(selectionQuery, (key, rid))
        result_list = []
        # Fetching result from cursor
        for result in cursor: result_list.append(result)
        rid = result_list[0][1]
        social_userid = result_list[0][2]
        language_id = result_list[0][3]
        is_picture_uploaded = result_list[0][4]
        message_by_language = str(language_id) + "_MESSAGES"
        # checking that password is present or not for the particular account
        if result_list[0][0]:
            try:
                # updation query for disconnecting user with facebook
                if is_picture_uploaded == picture_upload_status:
                   updateQuery = "UPDATE `users` SET `social_userid`=NULL , `is_fb_image`=%s WHERE `id`=%s"
                   # Executing the query
                   cursor.execute(updateQuery, (int(is_fb_image_flag), int(rid)))
                else:
                     updateQuery = "UPDATE `users` SET `social_userid`=NULL , `picture_url`=NULL , `is_fb_image`=%s WHERE `id`=%s"
                     # Executing the query
                     cursor.execute(updateQuery, (int(is_fb_image_flag), int(rid)))
            except:
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['UPDATING_USER_STATUS'], 500)
            
            # Commented the below code due to the remove of Facebook Friends Functionality    
            #try:
            #    # deleting friends of user that are associated with facebook account
            #    deletionQuery = "DELETE FROM `user_friends` WHERE `rid`=%s OR `friend_id`=%s OR `social_userid`=%s"
            #    # Executing the query
            #    cursor.execute(deletionQuery, (int(rid), str(social_userid), str(social_userid)))
            #except:
            #    logger.error(traceback.format_exc())
            #    return log_err (config[message_by_language]['DELETING_FRIENDS_STATUS'], 500)
            
            # returning the success json with success message and required data
            return {
                    'statusCode': 200,
                    'headers':{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                            },
                    'body': json.dumps({"is_connected":"false","auth":auth_token, "user_id":user_id})
                    }
        else:
            # returning the success json with message and required data when user is does not have password associated with account
            return {
                    'statusCode': 200,
                    'headers':{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                            },
                    'body': json.dumps({"is_connected":"true","auth":auth_token, "user_id":user_id})
                    }
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['USER_STATUS'], 500)
        
if __name__== "__main__":
    handler(None,None)