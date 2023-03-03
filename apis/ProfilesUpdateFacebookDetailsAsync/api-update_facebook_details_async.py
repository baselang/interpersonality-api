"""
API Module for updating user profile image.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- Fetching facebook access token from api
- get the updated users facebook image and than update the users image
- Returning the success json

"""

import json
import pymysql
import logging
import traceback
from os import environ
import configparser
from datetime import datetime
import urllib
import requests

message_by_language = "MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('update_facebook_details_async.properties', encoding = "ISO-8859-1")

# Getting environment variables
app_id = environ.get('APP_ID')
app_secret = environ.get('APP_SECRET')
facebook_access_token_url = environ.get('FACEBOOK_ACCESS_TOKEN_URL')
graph_api_url = environ.get('GRAPH_API_URL')

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

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

def handler(event,context):
    """Function to handle the request for updating picture_url of the user."""
    logger.info(event)
    # checking that the following event call is from lambda warmer or not        
    try:
        # Fetching event data from request event object
        rid = event['rid']
    except:
        # if above code fails than returning the json
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        try:
            # Constructing the query to get the social_userid of the user or fb_id related to user
            selectionQuery = "SELECT `social_userid` FROM `users` WHERE `id`=%s"
            # Executing the batch query
            cursor.execute(selectionQuery, (rid))
            
            user_list = []
            # fetching result from the cursor
            for result in cursor: user_list.append(result)
            # fetching fb_id or social_userid of the user from result_list
            fb_id = str(user_list[0][0])
            
            # preparing paload to make post request for access token
            access_token_payload =  {
                                       'grant_type': 'client_credentials',
                                       'client_id': app_id,
                                       'client_secret': app_secret
                                    }
            
            # making a post request to get an access token for making request to graph api
            access_token_response = requests.post(facebook_access_token_url, params=access_token_payload)
            
            # gettting access token from the response
            access_token = access_token_response.json()['access_token']
            
            
            # creating a graph api url for particular facebook id
            graph_url = graph_api_url + fb_id
            # preparing payload to make post request to scrap the users profile
            graph_payload = {
                                "fields":"picture",
                                "access_token":access_token
                            }
            
            # making a post request to graph api to scrap the users profile
            graph_payload_response = requests.get(graph_url, params=graph_payload)
            
            # converting the string response to json response
            graph_payload_response = graph_payload_response.json()
            
            # fetching the picture_url and friends list from the json response
            picture_url = graph_payload_response['picture']['data']['url']
            
            # Updating the picture_url of the user
            updationQuery = "UPDATE `users` SET `picture_url`=%s, `fb_data_updation_time`=NOW() WHERE `id`=%s AND `is_picture_uploaded`=0 AND DATE_ADD(`fb_data_updation_time`, INTERVAL 29 DAY) < NOW()"
            # Executing the batch query
            cursor.execute(updationQuery, (picture_url, rid))
            
            
            # returning the success json to the user 
            return  {
                        'statusCode': 200,
                        'headers': {
                                       'Access-Control-Allow-Origin': '*',
                                        'Access-Control-Allow-Credentials': 'true'
                                    },
                        'body': json.dumps({"message":config[message_by_language]['SUCCESS_MESSAGE']})
                    }
        except:
            # if any of the above code fails then returning the error
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
    finally:
        # Finally, clean up the connection
        cursor.close()
        cnx.close()
        
if __name__== "__main__":
    handler(None,None)