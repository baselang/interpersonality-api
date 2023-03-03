"""API Module to check health.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning error message
3. check_database_connection(): Check database is accessable
4. check_database_query_execution_status(): Check able to perform database query
5. check_active_campaign(): Check Active Campaign is accessable    
6. check_chargebee(): Check Chargebee is accessable
7. check_s3bucket(): Check s3bucket is accessable
8. check_facebook_app(): Check facebook app is accessable
9. send_email(): Sending emails
10. convert_string_to_list(): Convert string to list of string
11. handler(): Handling the incoming request with following steps:
- Check all integration in interpersonality. It is accessable or not.
- Based on it returning the JSON response with message and success status code

"""
import json
import pymysql
import logging
import requests
import traceback
import chargebee
import boto3
from botocore.client import Config
from os import environ
import facebook
import configparser
import calendar
from datetime import datetime
from datetime import timedelta

# getting messages according to languages
message_by_language = "MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('healthcheck.properties', encoding = "ISO-8859-1")  

# Getting the DB details from the environment variables to connect to DB.
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# Getting the database Secret key and secret key for getting token
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')
key = environ.get('DB_ENCRYPTION_KEY')

#Environment variable required for active campaign
AC_BASE_URL = environ.get('AC_BASE_URL')
API_TOKEN = environ.get('API_TOKEN')
GET_TAG_URL = AC_BASE_URL + environ.get('GET_TAG_URL')

#Environment variable required for chargebee
SITE_KEY = environ.get('SITE_KEY')
SITE_URL = environ.get('SITE_URL')
BUCKET_NAME = environ.get('BUCKET_NAME')

#Environment variables related to s3 bucket
ACCESS_KEY_ID = environ.get('ACCESS_KEY_ID')
SECRET_ACCESS_KEY = environ.get('SECRET_ACCESS_KEY')

#Getting Facebook app related environment variables
app_id = environ.get('APP_ID')
app_secret = environ.get('APP_SECRET')
facebook_access_token_url = environ.get('FACEBOOK_ACCESS_TOKEN_URL')
graph_api_url = environ.get('GRAPH_API_URL')
fb_user_id = environ.get('FB_USER_ID')

#Environment variables related to send email
AWS_REGION = environ.get('REGION')
SENDER = environ.get('SENDER')
RECIPIENT = environ.get('RECIPIENT')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')
LOGS_URL = environ.get('LOGS_URL')


#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
#Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()

# Setting the log level to INFO
logger.setLevel(logging_Level)

logger.info("Cold start complete.") 

def make_connection():
    """Function to make the database connection."""
    return pymysql.connect(host=endpoint, user=dbuser, passwd=password,
        port=int(port), db=database, autocommit=True)

def log_err(errmsg):
    """Function to log the error messages."""
    logger.error(errmsg)    
    return "ERROR"

def check_database_connection():
    """Function to check database is accessable"""
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
    except:
        logger.error(traceback.format_exc())            
        return log_err(config[message_by_language]['CONNECTION_STATUS'])

def check_database_query_execution_status():
    """Function to check able to perform query in database"""
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()

        # Query for getting user_gender id
        selectionQuery = "SELECT `id` FROM `user_gender`"
        # Executing the Query
        cursor.execute(selectionQuery)
        
        result_list = []
        # fetching result from the cursor
        for result in cursor: result_list.append(result)
        
        # getting user_gender id
        user_gender_id = result_list[0][0]
        logger.info("user_gender_id")
        logger.info(user_gender_id)
        return "OK"
    except:
        logger.error(traceback.format_exc())        
        return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'])


def check_active_campaign():
    """Function to check Active Campaign is accessable"""    
    try:
        # defining a params dict for the parameters to be sent to the API rid
        HEADERS = {'Api-Token': API_TOKEN}
        # sending get request
        response = requests.get(url=GET_TAG_URL, headers=HEADERS)
        logger.info("response.status_code")
        logger.info(response.status_code)
        if(response.status_code == 200):
            return "OK"
        else:
            logger.error(traceback.format_exc())            
            return log_err (config[message_by_language]['ACTIVE_CAMPAIGN_ACCESSIBLE_STATUS'])              
    except:
        logger.error(traceback.format_exc())        
        return log_err (config[message_by_language]['ACTIVE_CAMPAIGN_ACCESSIBLE_STATUS'])

def check_chargebee():
    """Function to check Chargebee is accessable"""    
    try:
        # configuring chargebee object
        chargebee.configure(SITE_KEY, SITE_URL)
        logger.info("chargebee")
        logger.info(chargebee)

        # getting the subscriptions related to the customer from the chargebee
        entries = chargebee.Customer.list({
            "first_name[is]" : "John",
            "last_name[is]" : "Doe",
            "email[is]" : "john@test.com"
            })
        logger.info("entries")
        logger.info(entries)
        return "OK"
    except:
        logger.error(traceback.format_exc())        
        return log_err (config[message_by_language]['CHARGEBEE_ACCESSIBLE_STATUS'])  

def check_s3bucket():
    """Function to check s3bucket is accessable"""    
    try:
        # creating boto3 client 
        S3 = boto3.resource(
            's3',
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=SECRET_ACCESS_KEY,
            config=Config(signature_version='s3v4')
            )
        my_bucket = S3.Bucket(BUCKET_NAME)
        #Checking bucket objects is accessable or not 
        for file in my_bucket.objects.all():
            logger.info("file.key")
            logger.info(file.key)
            break;
        return "OK"
    except:
        logger.error(traceback.format_exc())        
        return log_err (config[message_by_language]['S3_BUCKET_ACCESSIBLE_STATUS'])


def check_facebook_app():
    """Function to check facebook app is accessible"""
    try:
        # preparing payload to make post request for access token
        access_token_payload =  {
                                   'grant_type': 'client_credentials',
                                   'client_id': app_id,
                                   'client_secret': app_secret
                                }

        # making a post request to get an access token for making request to graph api
        access_token_response = requests.post(facebook_access_token_url, params=access_token_payload)

        # getting access token from the response
        access_token = access_token_response.json()['access_token']

        logger.info("access_token")
        logger.info(access_token)

        # creating a graph api url for particular facebook id
        graph_url = graph_api_url + str(fb_user_id)

        logger.info("graph_url")
        logger.info(graph_url)
        # preparing payload to make post request to fetch information from facebook
        graph_payload = {
                "fields":"name,picture",
                "access_token":access_token
            }

        graph_payload_response = requests.get(graph_url, params=graph_payload)
        if(graph_payload_response.status_code == 200):
            return "OK"
        else:
            logger.error(traceback.format_exc())            
            return log_err (config[message_by_language]['FACEBOOK_APP_ACCESSIBLE_STATUS'])
    except:
        logger.error(traceback.format_exc())        
        return log_err (config[message_by_language]['FACEBOOK_APP_ACCESSIBLE_STATUS'])

def convert_string_to_list(string): 
    li = list(string.split(",")) 
    return li

def send_email(RECIPIENT, SUBJECT_LINE, SUBJECT_BODY):
    """Function for sending emails """

    #get the current day
    day = calendar.day_name[datetime.utcnow().weekday()]
    
    # The HTML body of the email.
    BODY_HTML = (config[message_by_language]['EMAIL_BODY_TEMPLATE']).format(SUBJECT_BODY,LOGS_URL,day)
    
    # The character encoding for the email.
    CHARSET = "UTF-8"
    
    client = boto3.client(
        'ses',
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    
    # Try to send the email.
    #Provide the contents of the email.
    response = client.send_email(
            Destination={
                'ToAddresses': convert_string_to_list(RECIPIENT),
            },
            Message={
                'Body': {
                    'Html': {
                    'Charset': CHARSET,
                    'Data': BODY_HTML,
                    }
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT_LINE,
                },
            },
            Source=SENDER,
        )
    logger.info(response)
    print(response)
    return             


def handler(event,context):
    """Function to handle the request and check all integration in interpersonality functionality working fine"""
    global message_by_language
    try:
        logger.info(event)
        status = "OK"
        msg = []
        status = check_database_connection()
        logger.info(status)

        if status == "ERROR":
            send_email(RECIPIENT, config[message_by_language]['SUBJECT_LINE_DB_CONNECTION'],config[message_by_language]['SUBJECT_BODY_DB_CONNECTION'])
            msg.append(config[message_by_language]['ERROR_MESSAGE']+" "+config[message_by_language]['SUBJECT_BODY_DB_CONNECTION'])

        status = check_database_query_execution_status()

        if status == "ERROR":
            send_email(RECIPIENT, config[message_by_language]['SUBJECT_LINE_DB_QUERY_EXECUTION'],config[message_by_language]['SUBJECT_BODY_DB_QUERY_EXECUTION'])
            msg.append(config[message_by_language]['ERROR_MESSAGE']+" "+config[message_by_language]['SUBJECT_BODY_DB_QUERY_EXECUTION'])

        status = check_active_campaign()
        
        if status == "ERROR":
            send_email(RECIPIENT, config[message_by_language]['SUBJECT_LINE_ACTIVE_CAMPAIGN'],config[message_by_language]['SUBJECT_BODY_ACTIVE_CAMPAIGN'])
            msg.append(config[message_by_language]['ERROR_MESSAGE']+" "+config[message_by_language]['SUBJECT_BODY_ACTIVE_CAMPAIGN'])

        status = check_chargebee()

        if status == "ERROR":
            send_email(RECIPIENT, config[message_by_language]['SUBJECT_LINE_CHARGEBEE'],config[message_by_language]['SUBJECT_BODY_CHARGEBEE'])
            msg.append(config[message_by_language]['ERROR_MESSAGE']+" "+config[message_by_language]['SUBJECT_BODY_CHARGEBEE'])

        status = check_s3bucket()

        if status == "ERROR":
            send_email(RECIPIENT, config[message_by_language]['SUBJECT_LINE_S3_BUCKET'],config[message_by_language]['SUBJECT_BODY_S3_BUCKET'])
            msg.append(config[message_by_language]['ERROR_MESSAGE']+" "+config[message_by_language]['SUBJECT_BODY_S3_BUCKET'])

        status = check_facebook_app()
    
        if status == "ERROR":
            send_email(RECIPIENT, config[message_by_language]['SUBJECT_LINE_FACEBOOK_APPLICATION'],config[message_by_language]['SUBJECT_BODY_FACEBOOK_APPLICATION'])
            msg.append(config[message_by_language]['ERROR_MESSAGE']+" "+config[message_by_language]['SUBJECT_BODY_FACEBOOK_APPLICATION'])

        if msg:
            msg = '\n'.join(msg)
            logger.info(msg)
            return msg
        else:
            logger.info(status)
            return status

    except:
        logger.error(traceback.format_exc())
        return "ERROR"
    finally:
        try:                 
            # Finally, clean up the connection
            cnx.close()
            cursor.close()
        except: 
            pass

if __name__== "__main__":
    handler(None,None)
