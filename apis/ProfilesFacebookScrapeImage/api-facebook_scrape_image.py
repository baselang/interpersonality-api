"""
API Module to make a call to generate profile image and scrape the profile url by making request to Facebook graph api.

It provides the following functionalities:
1. log_err(): Logging error and returning the JSON response with error message & status code
2. make_connection(): To prepare a connection object for pymysql
3. make_client(): To make a boto3 client object for invoking lambda function
2. handler(): Handling the incoming request with following steps:
- fetching the data from event
- making a call to generate image url
- saving the image_url returned after call into the database
- making a scrape url which should be language specific
- Fetching facebook access tocken from api
- Making request to scrape the url provided by the user
- Returning the success json

"""

import json
import logging
import traceback
from os import environ
import urllib
import requests
import configparser
import boto3
import pymysql

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('facebook_scrape_image.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# Getting environment variables
app_id = environ.get('APP_ID')
app_secret = environ.get('APP_SECRET')
facebook_access_token_url = environ.get('FACEBOOK_ACCESS_TOKEN_URL')
graph_api_url = environ.get('GRAPH_API_URL')

# Variables related to s3 bucket
AWS_REGION =  environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')
PROFILES_LINK = environ.get('PROFILES_LINK')
ENVIRONMENT_URL = environ.get('ENVIRONMENT_URL')

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
    return  {
                "statusCode": status_code,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def make_client():
    """Making a boto3 aws client to perform invoking of functions"""
    
    # creating an aws client object by providing different cridentials
    invokeLam = boto3.client(
                                "lambda", 
                                region_name=AWS_REGION,
                                aws_access_key_id=AWS_ACCESS_KEY,
                                aws_secret_access_key=AWS_SECRET
                            )
    # returning the object
    return invokeLam

def handler(event,context):
    """Function to handle the request for Scraping URL."""
    logger.info(event)
    # checking that the following event call is from lambda warmer or not        
    try:
        # Fetching event data from request event object
        global message_by_language
        rid = int(event['rid'])
    except:
        # if above code fails than returning the json
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)     
    
    try:    
        # making an boto 3 client object
        invokeLam = make_client()
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['INVOCATION_ERROR'], 500)
        
    try:
        # preparing payload for the lambda call
        payload = {'rid' : int(rid)}
        # calling the lambda function synchronously to generate an generic image for similarity for sharing on facebook
        response = invokeLam.invoke(FunctionName="ProfilesGenericProfileImage" + ENVIRONMENT_TYPE, InvocationType="RequestResponse", Payload=json.dumps(payload))
        # getting the payload from the response
        response = response['Payload']
        # decoding the response payload
        response = json.loads(response.read().decode("utf-8"))
        # getting image_url from response
        image_url = json.loads(response['body'])['image_url']
        logger.info(image_url)
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['INVOCATION_ERROR'], 500)
        
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        try:
            # updating the users to put the scrape_image_url
            updationQuery = "UPDATE `users` SET `scrape_image_url`=%s WHERE `id`=%s"
            # Executing the query using
            cursor.execute(updationQuery, (image_url, int(rid)))
            
            # Query for getting current language of the user
            selectionQuery = "SELECT `language_id`,`user_id` FROM `users` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (rid))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            if len(result_list) == 0:
                return log_err(config[message_by_language]['INVALID_USER'], 404)
            
            # getting current language_id of the user
            language_id = result_list[0][0]
            user_id = result_list[0][1]
            message_by_language = str(language_id) + "_MESSAGES"
            
            if int(language_id) == 245:
                # if language is spanish than preparing the scrape_url
                scrape_url = ENVIRONMENT_URL + "es/" + PROFILES_LINK + user_id
            elif language_id == 165:
                # if language is english than preparing the scrape_url
                scrape_url = ENVIRONMENT_URL + PROFILES_LINK + user_id
            logger.info(scrape_url) 
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
    
    try:
        # preparing paload to make post request for access token
        access_token_payload =  {
                                   'grant_type': 'client_credentials',
                                   'client_id': app_id,
                                   'client_secret': app_secret
                                }
                                
        # making a post request to get an access token for making request to scrap
        access_token_response = requests.post(facebook_access_token_url, params=access_token_payload)
        
        # gettting access token from the response
        access_token = access_token_response.json()['access_token']
        
        # preparing payload to make post request to scrap the users profile
        graph_payload = {
                            'scrape' : 'true',
                            'id': scrape_url,
                            'access_token': access_token
                        }
        
        # making a post request to graph api to scrap the users profile
        graph_payload_response = requests.post(graph_api_url, params=graph_payload)
        
        if graph_payload_response.status_code == 200:
            # returning the success json to the user 
            return {
                        'statusCode': 200,
                        'headers': {
                                       'Access-Control-Allow-Origin': '*',
                                        'Access-Control-Allow-Credentials': 'true'
                                    },
                        'body': json.dumps({"message":(config[message_by_language]['SUCCESS_MESSAGE']).format(scrape_url)})
                    }    
        else:
            # if status code is not success status code than printing the issue and returning the error
            logger.info(graph_payload_response)
            return log_err (graph_payload_response, graph_payload_response.status_code)
    except:
        # if any of the above code fails then returning the error
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)   
        
if __name__== "__main__":
    handler(None,None)