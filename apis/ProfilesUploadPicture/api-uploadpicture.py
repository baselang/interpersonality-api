"""API For uploading user profile picture.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. upload_image(): function for uploading image to aws and returning url of uploaded image
5. handler(): Handling the incoming request with following steps:
- Fetching data from request
- uploading picture to s3 and updating url of s3 image to database using upload_image()
- Returning the JSON response with success status code with the message ,authentication token and user_id in the response body
"""

import jwt
import json
import pymysql
import logging
import traceback
from os import environ
import configparser
import boto3
from botocore.client import Config
import base64

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('uploadpicture.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# Variables related to s3 bucket
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')
BUCKET_NAME = environ.get('BUCKET_NAME')


# secret keys for image upload for creating boto3 client
ACCESS_KEY_ID = environ.get('ACCESS_KEY_ID')
SECRET_ACCESS_KEY = environ.get('SECRET_ACCESS_KEY')
BUCKET_NAME = environ.get('BUCKET_NAME')
S3_BUCKET_URL = environ.get('S3_BUCKET_URL')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')
is_picture_uploaded_flag = int(environ.get('is_picture_uploaded_flag'))
is_fb_image_flag = int(environ.get('is_fb_image_flag'))

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

def upload_image(user_id, image_data):
    """Function to upload image to S3 and generate url"""
    # creating boto3 client 
    S3 = boto3.resource(
        's3',
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4')
        )
    
    # splitting user provided image data to convert into image format
    ans = image_data.split("base64,", 1)
    image_data = ans[1]
    
    # converting string to byte format
    image_data = image_data.encode("utf-8")
    
    # converting byte format to base64 format
    image_data = base64.decodebytes(image_data)
    
    # uploading image to S3 bucket
    response = S3.Object(BUCKET_NAME,user_id + ".png").put(Body=image_data, ACL='public-read-write')
    logger.info(response)
    
    # returning S3 url of the image
    return S3_BUCKET_URL + user_id + ".png"

def handler(event,context):
    """Function to handle the request for upload picture API"""
    global message_by_language
    logger.info(event)
    try:
        # Fetching data from event and rendering it
        auth_token = event['headers']['Authorization']
        body = json.loads(event['body'])
        #body = event['body']
        picture_data = body['picture_data']
        
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
        
        try:
            # uploading picture to s3 if it is not the same as old and generating url of it
            try:
                picture_url = upload_image(user_id, picture_data)
            except Exception as e:
                return {
                    'statusCode': 400,
                    'headers': {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Credentials': 'true'
                    },
                    'body': json.dumps({"message": config[message_by_language]['UPLOAD_IMAGE_ERROR']})
                }

            logger.info(picture_url)
            # Query for updating picture_url of user
            updationQuery = "UPDATE `users` SET `picture_url`=%s, `is_picture_uploaded`=%s, `is_fb_image`=%s WHERE `id`=%s"
            # Executing the Query
            cursor.execute(updationQuery, (picture_url, is_picture_uploaded_flag, is_fb_image_flag, rid))
            # cursor.execute(updationQuery, (picture_url, is_picture_uploaded_flag, is_fb_image_flag, rid))
        except:
            logger.info(traceback.format_exc())
            return log_err (config[message_by_language]['IMAGE_STATUS'], 500)
        # calling the lambda function synchronously to generate an generic profile image for sharing on facebook
        try:
            # creating a boto3 service client object
            invokeLam = boto3.client(
                "lambda",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY,
                aws_secret_access_key=AWS_SECRET
            )
        except:
            # when there is some problem in above code
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['BOTO_SERVICE_CLIENT_STATUS'], 500)

        try:
            # preparing payload for the lambda call
            payload = {'rid': int(rid)}

            # calling the lambda function asynchronously to generate an generic profile image for sharing on facebook
            invokeLam.invoke(FunctionName="ProfilesFacebookScrapeImage" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
        except:
            # when there is some problem in above code
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['INVOKING_ASYNC_STATUS'], 500)
            

        # returning success json
        return {
                    'statusCode': 200,
                    'headers':{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                            },
                    'body': json.dumps({"picture_url": picture_url})
                }
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
        
if __name__== "__main__":
    handler(None,None)