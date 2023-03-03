"""
API to regenerate deprecated Images of facebook.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching data from request
- Getting friends_user_id and checking weather has deprecated image or not.
- If yes then generating the new image from facebook and saving it to database.
"""
import requests
import facebook
import pymysql
import configparser
import logging
import json
import traceback
from os import environ

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('userregeneratefacebookimages.properties', encoding = "ISO-8859-1")

# getting message variable
message_by_language = "165_MESSAGES"

IMAGE_SIZE = environ.get("IMAGE_SIZE")
app_id = environ.get("APP_ID")
app_secret = environ.get("APP_SECRET")
facebook_access_token_url = environ.get("TOKEN_URL")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get("ENDPOINT")
port = environ.get("PORT")
dbuser = environ.get("DBUSER")
password = environ.get("DBPASSWORD")
database = environ.get("DATABASE")

# secret keys for data encryption and security token
key = environ.get("DB_ENCRYPTION_KEY")
SECRET_KEY = environ.get("TOKEN_SECRET_KEY")

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# Getting the logger to log the messages for debugging purposes
logger = logging.getLogger()
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

def handler(event,context):
    """Function to handle the request for Scraping URL."""
    logger.info(event)
    # checking that the following event call is from lambda warmer or not
    try:
        # Fetching event data from request event object
        global message_by_language
        user_id = event['headers']['user_id']

    except:
        # if above code fails than returning the json
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)

    try:
        # Making the DB connection
        cnx = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()

        access_token_payload = {
                                   'grant_type': 'client_credentials',
                                   'client_id': app_id,
                                   'client_secret': app_secret
                                }
        access_token_response = requests.post(facebook_access_token_url, params=access_token_payload)
        access_token = access_token_response.json()['access_token']

        selectionQuery = "SELECT `social_userid`, `is_fb_image` FROM `users` WHERE `user_id`=%s"
        cursor.execute(selectionQuery, (user_id))

        result_list = []
        # fetching result from the cursor
        for result in cursor: result_list.append(result)

        social_userid = result_list[0][0]
        is_fb_image = result_list[0][1]

        if is_fb_image != 1:
            # Fetching user data from facebook by using access_token
            graph = facebook.GraphAPI(access_token)
            profile = graph.get_object(social_userid, fields='picture.width(' + str(IMAGE_SIZE) + ').height(' + str(IMAGE_SIZE) + ')')
            picture_url = profile['picture']['data']['url']

            # Updating new picture url in database
            updateQuery = "UPDATE `users` SET `picture_url` = %s WHERE `user_id`=%s"
            cursor.execute(updateQuery, (picture_url, user_id))


    except:
        logger.info(traceback.format_exc())
        # If there is any error in above operations, logging the error
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)

    finally:
        try:
            # Finally, clean up the connection
            cursor.close()
            cnx.close()
        except:
            pass

