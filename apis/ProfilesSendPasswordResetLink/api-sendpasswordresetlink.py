#!/usr/bin/env python3

"""API Module to send password reset link to email id for the account associated with that email id.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. send_email2(): Sends email through sender email id to the email id provided by user 
4. handler(): Handling the incoming request with following steps:
- finding user associated with the provided email id
- generating a link to be send to user for reset password
- Returning the JSON response with success status code and success messages

"""

import json
import pymysql
import logging
import traceback
from os import environ
from datetime import datetime
from datetime import timedelta
import calendar
from pyDes import *
import boto3
from botocore.exceptions import ClientError
import configparser

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('sendpasswordresetlink.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')
key = environ.get('DB_ENCRYPTION_KEY')

# aws cridentials required for sending an Email through Amazon SES
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
SENDER = environ.get('SENDER')
reset_password_link = environ.get('RESET_PASSWORD_LINK')
link_timeout = environ.get('RESET_LINK_TIMEOUT')
environment_url = environ.get('ENVIRONMENT_URL')

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

def log_err(errmsg , status_code):
    """Function to log the error messages."""
    logger.error(errmsg)
    return  {
                "statusCode": status_code,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def send_email2(RECIPIENT, link, firstname):
    """Function for sending emails """
    # The subject line for the email.
    SUBJECT = (config[message_by_language]['EMAIL_SUBJECT'])
    
    #get the current day
    day = calendar.day_name[datetime.utcnow().weekday()]
    # The HTML body of the email.
    BODY_HTML = (config[message_by_language]['EMAIL_BODY_TEMPLATE']).format(firstname,link,day)
    
    # The character encoding for the email.
    CHARSET = "UTF-8"
    
    # creating a boto3 client.
    client = boto3.client(
        'ses',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
    )
    
    # Try to send the email.
    #Provide the contents of the email.
    response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
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
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    return

def handler(event,context):
    """Function to handle the request for Send Password Reset Link API."""
    global message_by_language
    global reset_password_link
    try:
        # Fetching data from event body
        data = json.loads(event['body'])
        email = data['email'].lower()
        language_id = event['headers']['language_id']
        message_by_language = str(language_id) + "_MESSAGES"
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        try:
            # Constructing the query to get user with email address 
            selectionQuery = "SELECT `id`,cast(AES_DECRYPT(`firstname`,%s) as char),`language_id` FROM `users` WHERE `id`=(SELECT `rid` FROM `user_emails` where email=AES_ENCRYPT(%s, %s))"
            # Executing cursor
            cursor.execute(selectionQuery, (key, email, key))
            try:
                # Taking out id from cursor if user with given email id exist
                result_list = []
                for result in cursor: result_list.append(result)
                
                # getting language_id of the user
                language_id = result_list[0][2]
                
                # creating the content which is to be sent in url for authentication of url
                payload = {}
                payload['id'] = result_list[0][0]
                payload['exp'] = datetime.timestamp(datetime.now() + timedelta(minutes=int(link_timeout)))
                payload = json.dumps(payload)
                
                # getting language code according to language_id
                if int(language_id) != 165:
                    try:
                        # Constructing the query to get language_code of the user from language table
                        selectionQuery = "SELECT `code` FROM `language` WHERE `id`=%s"
                        # Executing cursor
                        cursor.execute(selectionQuery, (int(language_id)))
                        
                        language_results_list = []
                        # fetching the result from the cursor
                        for language_results in cursor: language_results_list.append(language_results)
                        # getting language code and changing it to url form to include it in the url
                        language_code = "/" + language_results_list[0][0]
                    except:
                        # If there is any error in above operations, logging the error
                        logger.error(traceback.format_exc())
                        return log_err(config[message_by_language]['LANGUAGE_CODE_STATUS'], 500)
                else:
                    # getting language code and changing it to url form to include it in the url
                    language_code = ""
                    
                
                # Encoding the content into payload using key
                ciphertext1 = (triple_des(key).encrypt(payload, padmode=2)).hex()
                
                # Creating link to be sent to email address
                ans =  environment_url + language_code + reset_password_link + str(result_list[0][2]) + '.' + ciphertext1
                
                logger.info(ans)
                
                # inserting token for validity checking into users table and setting its access True
                try:
                    insertionQuery = "UPDATE `users` SET `token`=%s,`isActiveToken`=1 WHERE `id`=%s"
                    cursor.execute(insertionQuery, (ciphertext1, result_list[0][0]))
                except:
                    # If there is any error in above operations, logging the error
                    logger.error(traceback.format_exc())
                    return log_err(config[message_by_language]['TOKEN_TO_DATABASE'], 500)
                    
                try:
                    # function for sending email
                    send_email2(email, ans, result_list[0][1])
                except:
                    # If there is any error in above operations, logging the error
                    logger.error(traceback.format_exc())
                    return log_err (config[message_by_language]['EMAIL_STATUS'], 500)
                
                # Returning JSON response           
                return {
                            'statusCode': 200,
                            'headers':{
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                            },
                            'body': json.dumps({"message":config[message_by_language]['SUCCESS_MESSAGE'], "language_id":language_id})
                        }
            except:
                # If there is any error in above operations, logging the error
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['EMAIL_EXISTENCE_STATUS'], 400)
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
    finally:
        try:
            # Finally, clean up the connection
            cursor.close()
            cnx.close()
        except:
            pass

if __name__== "__main__":
    handler(None,None)