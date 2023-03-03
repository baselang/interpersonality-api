"""API For making call to make call to generate report asynchronously and to scrape the shared url

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. make_client(): function for making a boto3 client 
5. handler(): Handling the incoming request with following steps:
- Fetching data from request
- calling api to generate interpersonal report and api to generate image and scrape shared url
- Returning the JSON response with success status code with the required data
"""

import jwt
import json
import pymysql
import logging
import traceback
from os import environ
import configparser
import boto3

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('interpersonal.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for image upload for creating boto3 client
ACCESS_KEY_ID = environ.get('ACCESS_KEY_ID')
SECRET_ACCESS_KEY = environ.get('SECRET_ACCESS_KEY')
AWS_REGION = environ.get('REGION')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')

# variables to make the url that is needs to be shared
ENVIRONMENT_URL = environ.get('ENVIRONMENT_URL')
PROFILES_LINK = environ.get('PROFILES_LINK')

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
    """Making a boto3 aws client to perform invoking of functions"""
    
    # creating an aws client object by providing different cridentials
    invokeLam = boto3.client(
                                "lambda", 
                                region_name=AWS_REGION,
                                aws_access_key_id=ACCESS_KEY_ID,
                                aws_secret_access_key=SECRET_ACCESS_KEY
                            )
    # returning the object
    return invokeLam

def handler(event,context):
    """Function to handle the request for upload picture API"""
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
        # If there is any error in above operations
        pass

    logger.info(event)
    try:
        # Fetching data from event and rendering it
        auth_token = event['headers']['Authorization']
        body = json.loads(event['body'])
        user_id_2 = body['user_id']
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
    
    try:
        # verifying that the user is authorized or not to see this api's data
        rid, user_id_1, language_id = jwt_verify(auth_token)
        message_by_language = str(language_id) + "_MESSAGES"
    except:
        # if user does not have valid authorization
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['UNAUTHORIZED'], 403)
    
    try:
        
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
                
                # Query for getting current language of the user
                selectionQuery = "SELECT `id` FROM `users` WHERE `user_id`=%s"
                # Executing the Query
                cursor.execute(selectionQuery, (user_id_2))
                
                result_list = []
                # fetching result from the cursor
                for result in cursor: result_list.append(result)
                
                # getting current language_id of the user
                friends_rid = int(result_list[0][0])
            except:
                # If there is any error in above operations, logging the error
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
            
        try:
            # making an boto 3 client object
            invokeLam = make_client()
            # getting the payload for python
            payload = {"headers": {"Authorization": auth_token, "lambda_source":"invoked_lambda", "user_id_2":user_id_2, "language_id":int(language_id)}}
            # invoking the lambda function with custom payload
            response = invokeLam.invoke(FunctionName= "ProfilesGenerateInterpersonalReport" + ENVIRONMENT_TYPE, InvocationType="RequestResponse", Payload=json.dumps(payload))
            response = response['Payload']
            response = json.loads(response.read().decode("utf-8"))
                
            # getting response
            final_json = json.loads(response['body'])
            similarity_score = final_json['similarity_score']
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['INVOCATION_ERROR'], 500)
            
        try:
            # making an boto 3 client object
            invokeLam = make_client()
            # getting the payload for python
            payload = {"rid":rid, "friends_rid":friends_rid, "similarity_score":similarity_score}
            # invoking the lambda function with custom payload
            response = invokeLam.invoke(FunctionName= "ProfilesScrapeInterpersonalSharedUrl" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['INVOCATION_ERROR'], 500)
            
            
        if int(language_id) == 245:
            # if language is spanish than preparing the scrape_url
            url_to_share = ENVIRONMENT_URL + "es/" + PROFILES_LINK + user_id_1 + "/" + user_id_2
        elif language_id == 165:
            # if language is english than preparing the scrape_url
            url_to_share = ENVIRONMENT_URL + PROFILES_LINK + user_id_1 + "/" + user_id_2
            
        final_json['url_to_share'] = url_to_share
        
        # returning the success json to the user 
        return {
                'statusCode': 200,
                'headers':  {
                               'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                            },
                'body': json.dumps(final_json)
                }
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
        
if __name__== "__main__":
    handler(None,None)