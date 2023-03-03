"""API For updating primary email of an user.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching data from request
- updating primary email of an user
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
import chargebee
from botocore.client import Config

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('updateprimaryemail.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

# Variables related to s3 bucket
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')
BUCKET_NAME = environ.get('BUCKET_NAME')

# Environment required for chargebee
SITE_KEY = environ.get('SITE_KEY')
SITE_URL = environ.get('SITE_URL')

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
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET
    )

    # returning the object
    return invokeLam

def handler(event,context):
    """Function to handle the request for updateprimaryemail API"""
    global message_by_language
    logger.info(event)
    try:
        # Fetching data from event and rendering it
        auth_token = event['headers']['Authorization']
        body = json.loads(event['body'])
        #body = event['body']
        primary_email = body['email']
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
        
    try:
        # verifying that the user is authorized or not to see this api's data
        rid, user_id, language_id = jwt_verify(auth_token)
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
        selectionQuery = "SELECT `language_id`, `ac_contact_id`,`customer_id` FROM `users` WHERE `id`=%s"
        # Executing the Query
        cursor.execute(selectionQuery, (rid))
        
        result_list = []
        # fetching result from the cursor
        for result in cursor: result_list.append(result)
        
        # getting current language_id of the user 
        language_id = result_list[0][0]
        ac_contact_id = result_list[0][1]
        customer_id = result_list[0][2]

        message_by_language = str(language_id) + "_MESSAGES"
        
        # Query for fetching picture_url of an user
        selectionQuery = "SELECT CAST(AES_DECRYPT(`email`,%s) AS CHAR) from `user_emails` WHERE `rid`= %s"
        # Executing the query
        cursor.execute(selectionQuery, (key, rid))
        
        ans_list = []
        # Fetching result in cursor from above query execution
        for result in cursor: ans_list.append(result[0])
        
        if primary_email in ans_list:
            # Query for updating users primary email
            updationQuery = "UPDATE `users` SET `primary_email`=AES_ENCRYPT(%s,%s) WHERE `id`=%s"
            # Executing the query
            cursor.execute(updationQuery, (primary_email, key, rid))

            try:
                # configuring chargebee object
                chargebee.configure(SITE_KEY, SITE_URL)
                # Updating Chargebee customer locale language

                if customer_id != None:
                    result = chargebee.Customer.update(customer_id, {
                        "email": primary_email,
                    })

            except:
                logger.error(traceback.format_exc())

        else:
            return log_err (config[message_by_language]['EMAIL_STATUS'], 500)


        if ac_contact_id != None:
            try:
                # making an boto 3 client object
                invokeLam = make_client()

                # preparing the payload for lambda invocation
                payload = {"headers":{"Authorization":auth_token, "user_partner_id": None}}

                invokeLam.invoke(FunctionName= "ProfilesActiveCampaignUpdate" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))

            except:
                # If there is any error in above operations, logging the error
                logger.error(traceback.format_exc())
                return log_err(config[message_by_language]['INVOCATION_ERROR'], 500)

            
            
        # returning success json
        return {
                    'statusCode': 200,
                    'headers':{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                            },
                    'body': json.dumps({"message": config[message_by_language]['SUCCESS_MESSAGE']})
                }
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
        
if __name__== "__main__":
    handler(None,None)