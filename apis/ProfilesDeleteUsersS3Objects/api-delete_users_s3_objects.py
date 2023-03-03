"""API For deleting all the images related to a user.

It provides the following functionalities:
1. make_client(): Function for making boto3 s3 client
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. upload_image(): function for uploading image to aws and returning url of uploaded image
5. handler(): Handling the incoming request with following steps:
- Fetching data from request
- getting all the images that are related to the user
- deleting all the images related to the user
- Returning the JSON response with success status code
"""

import jwt
import json
import logging
import traceback
from os import environ
import configparser
import boto3
import pymysql

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('delete_users_s3_objects.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for image upload for creating boto3 client
ACCESS_KEY_ID = environ.get('ACCESS_KEY_ID')
SECRET_ACCESS_KEY = environ.get('SECRET_ACCESS_KEY')
BUCKET_NAME = environ.get('BUCKET_NAME')
PROFILES_IMAGE_TEMPLATE = environ.get('PROFILES_IMAGE_TEMPLATE')
USER_IMAGE_TEMPLATE = environ.get('USER_IMAGE_TEMPLATE')
SIMILARITY_IMAGE_TEMPLATE = environ.get('SIMILARITY_IMAGE_TEMPLATE')
SUPPORTED_LANGUAGES = environ.get('SUPPORTED_LANGUAGES')
DEFAULT_LANGUAGES = str(environ.get('DEFAULT_LANGUAGE'))

# secret keys for data encryption and security token
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# getting message variable

message_by_language = DEFAULT_LANGUAGES + "_MESSAGES"

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

def make_client():
    """Function to make boto3 s3 client"""
    
    # creating boto3 s3 client 
    client = boto3.client(
                            's3',
                            aws_access_key_id=ACCESS_KEY_ID,
                            aws_secret_access_key=SECRET_ACCESS_KEY,
                        ) 
    
    # returning the created object
    return client

def handler(event,context):
    """Function to handle the request for deleting all the pictures of a user"""
    global message_by_language
    logger.info(event)
    try:
        # Fetching data from event and rendering it
        auth_token = event['Authorization']
        interpersonal_result_list = event['interpersonal_result_list']
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
            selectionQuery = "SELECT `id` FROM `language` WHERE `id` IN ( " + SUPPORTED_LANGUAGES + " )"
            # Executing the Query
            cursor.execute(selectionQuery)
            
            language_id_list = []
            # fetching result from the cursor
            for result in cursor: language_id_list.append(int(result[0]))
            
            interpersonal_image_list = []
            
            # fetching the interpersonal image list
            for firstloop in interpersonal_result_list:
                if firstloop[0] == user_id or firstloop[1] == user_id:
                    for secondloop in language_id_list:
                        # appending the similarity image name to the interpersonal image list
                        interpersonal_image_list.append({"Key":SIMILARITY_IMAGE_TEMPLATE.format(str(secondloop) ,firstloop[0], firstloop[1])})
        except:
            # if the response status is not 200
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
    except:
        logger.error(traceback.format_exc())
        # If there is any error in above operations, logging the error
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)
    finally:
        # Finally, clean up the connection
        cursor.close()
        cnx.close()
            
    try:
        
        # making list of all the objects that we need to delete
        image_list = [{"Key":PROFILES_IMAGE_TEMPLATE.format(str(language), user_id)} for language in language_id_list]
        
        # appending the user image name
        image_list.append({"Key":USER_IMAGE_TEMPLATE.format(user_id)})
        
        image_list.extend(interpersonal_image_list)
        
        logger.info("image_list ::::::::::::::::::::")
        logger.info(image_list)
        
        # getting the boto3 s3 client
        client = make_client()
        
        # deleting all the objects from the
        response = client.delete_objects(Bucket = BUCKET_NAME, Delete = {'Objects': image_list})
        
        logger.info(response)
        
        # if the status code returned by the response object is 200
        if int(response['ResponseMetadata']['HTTPStatusCode']) == 200:
            # returning success json
            return {
                        'statusCode': 200,
                        'headers':{
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                                },
                        'body': json.dumps({"message": (config[message_by_language]['SUCCESS_STATUS']).format(user_id)})
                    }
        else:
            # if the response status is not 200
            return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
        
if __name__== "__main__":
    handler(None,None)