"""API Module to make user connect using facebook to our application with an existing account.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching facebook code from event
- Fetching faebook details of user using code
- updating user and user_emails with facebook details which we have fetched
- generating an access token so that user can be verified after signup and begin his session
- Returning the JSON response with success status code with the authentication token and user_id in the response body

"""

import json
import pymysql
import logging
import traceback
from os import environ
import urllib
import requests
import facebook
import jwt
from datetime import datetime  
from datetime import timedelta
import configparser
import boto3

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('facebookconnect.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
app_id = environ.get('APP_ID')
app_secret = environ.get('APP_SECRET')
redirect_url_fb = environ.get('REDIRECT_FB_URL')

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# aws cridentials required for sending an Email through Amazon SES
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
IMAGE_SIZE = environ.get('IMAGE_SIZE')

# Getting key for getting token
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')
key = environ.get('DB_ENCRYPTION_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')
TOKEN_EXPIRY_TIME = environ.get('TOKEN_EXPIRY_TIME')

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
    """Function to handle the request for facebook Connect API."""
    logger.info(event)
    global message_by_language
    global TOKEN_EXPIRY_TIME
    try:
        # Fetching event data from request event object
        code = event['headers']['code']
        auth_token = event['headers']['Authorization']
        logger.info(code)
    except:
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
        
    try:
        # verifying that the user is authorized or not to see this api's data
        rid, user_id, language_id = jwt_verify(auth_token)
        message_by_language = str(language_id) + "_MESSAGES"
    except:
        # if user does not have valid authorization
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['UNAUTHORIZED'], 403)
        
    try:
        # creating a boto3 service client object
        invokeLam = boto3.client(
                                "lambda", 
                                region_name=AWS_REGION,
                                aws_access_key_id=AWS_ACCESS_KEY,
                                aws_secret_access_key=AWS_SECRET
                                )
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['BOTO_SERVICE_CLIENT_STATUS'], 500)
        
    try:
        # Fetching user data from facebook by getting access_token using code
        my_url = "https://graph.facebook.com/oauth/access_token?"+"client_id=" + app_id +"&redirect_uri="+ urllib.parse.quote(redirect_url_fb) +"&client_secret=" + app_secret +"&code=" + code
        r = requests.get(url=my_url)
        logger.info(r.json())
        acc_token = r.json()['access_token']
        
        # Fetching user data from facebook by using acess_token
        graph = facebook.GraphAPI(acc_token)
        profile = graph.get_object('me', fields ='name, email, picture.width(' + str(IMAGE_SIZE) + ').height(' + str(IMAGE_SIZE) + '), first_name, last_name')
        fb_id = profile['id']
        name = profile['name']
        firstname = profile['first_name']
        lastname = profile['last_name']
        picture_url = profile['picture']['data']['url']
        
        try:
            # Making the DB connection
            cnx    = make_connection()
            # Getting the cursor from the DB connection to execute the queries
            cursor = cnx.cursor()
            
            try:
                # Query for getting current language and name of the user
                selectionQuery = "SELECT `language_id`, CAST(AES_DECRYPT(`name`,%s) AS CHAR), CAST(AES_DECRYPT(`primary_email`,%s) AS CHAR) FROM `users` WHERE `id`=%s"
                # Executing the Query
                cursor.execute(selectionQuery, (key,key, rid))
                
                result_list = []
                # fetching result from the cursor
                for result in cursor: result_list.append(result)
                
                # getting current language_id of the user 
                language_id = result_list[0][0]
                name = result_list[0][1]
                primary_email = result_list[0][2]
                message_by_language = str(language_id) + "_MESSAGES"
            except:
                # If there is any error in above operations, logging the error
                logger.info(traceback.format_exc())
                return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
             
            # checking email is present or not
            try:
                email = profile['email'].lower()
            except:
                email = primary_email
            
            try:
                # checking that facebook account already exists or not
                selectionQuery = "SELECT `id` FROM `users` WHERE `social_userid`=%s"
                # Executing the query
                cursor.execute(selectionQuery, (fb_id))
                result_list = []
                for result in cursor: result_list.append(result)
                logger.info(result_list[0][0])
                return {'statusCode': 400,
                            'headers': {
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                                },
                            'body': json.dumps({"message": config[message_by_language]['DUPLICATE_FACEBOOK']})
                    }
            except:
                    
                try:
                    # Inserting or updating the user details into the users table
                    query = "UPDATE `users` SET picture_url = CASE WHEN `picture_url` IS NULL THEN %s ELSE picture_url END, `social_userid`=%s WHERE `id`=%s"
                    # Executing the query
                    cursor.execute(query, (picture_url, str(fb_id), int(rid)))

                    selectionQuery = "SELECT CAST(AES_DECRYPT(`email`,%s) AS CHAR) FROM `user_emails` WHERE `rid`=%s"
                    cursor.execute(selectionQuery, (key, rid))

                    result_list = []
                    for result in cursor: result_list.append(result[0])

                    if email not in result_list:
                        insertQuery = "INSERT INTO `user_emails` (`rid`, `user_id`, `email`) VALUES (%s, %s ,AES_ENCRYPT(%s, %s))"
                        # Executing the query using
                        cursor.execute(insertQuery, (rid, user_id, email, key))

                except:
                    logger.error(traceback.format_exc())
                    return log_err(config[message_by_language]['INSERT_USER_STATUS'], 500)
                    
            # calling facebooksignup asynchronous partition
            logger.info(profile)
            # Commented the below code due to the remove of Facebook Friends Functionality
            #try:
            #    payload = {'profile':profile,'rid':rid}
            #    invokeLam.invoke(FunctionName="ProfilesFacebookSignInAsync" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
            #except:
            #    logger.error(traceback.format_exc())
            #    return log_err (config[message_by_language]['INVOKING_ASYNC_STATUS'], 500)
            
            # creating a payload for generating authentication token
            payload = {}
            payload['id'] = rid
            payload['name'] = name
            payload['language_id'] = language_id
            payload['user_id'] = user_id
            payload['exp'] = datetime.timestamp(datetime.now() + timedelta(days=int(TOKEN_EXPIRY_TIME)))
            try:
                # generating an authentication token of a user
                token =  jwt.encode(payload, SECRET_KEY)
                logger.info("token: " + str(token)[2:-1])
            except:
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['TOKEN_STATUS'])
            
            # Returning a json response to the request by using required data 
            return  {
                        'statusCode': 200,
                        'headers':{
                                    'Access-Control-Allow-Origin': '*',
                                    'Access-Control-Allow-Credentials': 'true'
                                  },
                        'body': json.dumps({'auth':token.decode('utf-8'), 'user_id':user_id})
                    }
        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)
            
    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['FACEBOOK_DATA_STATUS'], 500)

if __name__== "__main__":
    handler(None,None)