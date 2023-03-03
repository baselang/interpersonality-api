"""API Module to Login to our application by using facebook login.

It provides the following functionalities:
1. log_err(): Logging error and returning the JSON response with error message & status code
2. handler(): Handling the incoming request with following steps:
- Fetching facebook access tocken from api

"""

import json
import logging
import traceback
from os import environ
import urllib
import requests
import facebook
from datetime import datetime 
from datetime import timedelta
import configparser
import boto3
import uuid

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('scrape_image.properties') 
# Getting the DB details from the environment variables to connect to DB

app_id = environ.get('APP_ID')
app_secret = environ.get('APP_SECRET')
facebook_access_token_url = environ.get('FACEBOOK_ACCESS_TOKEN_URL')
generic_profile_share_url = environ.get('GENERIC_PROFILE_SHARE_URL')
graph_api_url = environ.get('GRAPH_API_URL')
similarity_score_share_url = environ.get('SIMILARITY_SCORE_SHARE_URL')
user_profile_share_url = environ.get('USER_PROFILE_SHARE_URL')


# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
# Setting the log level to INFO
logger.setLevel(logging.INFO)

logger.info("Cold start complete.") 


def log_err(errmsg):
    """Function to log the error messages."""
    return  {
                "statusCode": 500,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def handler(event,context):
    """Function to handle the request for scrapeimage."""
    logger.info(event)
    # checking that the following event call is from lambda warmer or not        
    try:
        # Fetching event data from request event object
        data = json.loads(event['body'])
        scrapetype = data['scrapetype']
        print(scrapetype)
        
        if scrapetype == "scrape1":
            print("Value of scrapetype",scrapetype)
            scrape_url = generic_profile_share_url
        elif scrapetype == "scrape2":
            print("Value of scrapetype",scrapetype)
            scrape_url = user_profile_share_url
        elif scrapetype == "scrape3":
            print("Value of scrapetype",scrapetype)
            scrape_url = similarity_score_share_url
        
        print("scrape_url",scrape_url)    
       
    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'])     
        
    print("app_id",app_id)
    print("app_secret",app_secret)
    print("facebook_access_token_url",facebook_access_token_url)
    print("generic_profile_share_url",generic_profile_share_url)
    print("graph_api_url", graph_api_url)
    print("similarity_score_share_url",similarity_score_share_url )
    print("user_profile_share_url",user_profile_share_url )
        
    try:
        # post request for access token
        access_token_payload = {
        'grant_type': 'client_credentials',
        'client_id': app_id,
        'client_secret': app_secret
        }
        access_token_response = requests.post(facebook_access_token_url, params=access_token_payload)
        access_token = access_token_response.json()['access_token']
        print("access_token",access_token)
        
        # post request for graph api.
        graph_payload = {
        'scrape' : 'true',
        'id': scrape_url,
        'access_token': access_token
        }
        print("graph_payload",graph_payload)
        graph_payload_response = requests.post(graph_api_url, params=graph_payload)
        print("response",graph_payload_response)
        print("graph_payload_response",graph_payload_response.status_code)
       
    except:
        logger.error(traceback.format_exc())
        logger.info(config[message_by_language]['INTERNAL_ERROR'])
        return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)   
        
    if graph_payload_response.status_code == 200:
        return {
            'statusCode': 200,
            'headers':
                {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
            'body': json.dumps({"message":"URL "+ scrape_url +" Scraped Successfully"})
        }    
    else:
        return {
            'statusCode': 500,
            'headers':
                {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
            'body': json.dumps({"message":"Request Failed"})
        }    
           

if __name__== "__main__":
    handler(None,None)