#!/usr/bin/env python3

"""API Module to Delete User Profile Functionalities.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. make_lambda_client(): Making a boto3 aws client to perform invoking of functions
5. send_email2(): Sends email through sender email id to the email id provided by user 
6. handler(): Handling the incoming request with following steps:
- Delete User Profile
- Returning the JSON response with success status code

"""

import calendar
import configparser
import json
import logging
import traceback
from datetime import datetime
from os import environ

import boto3
import jwt
import pymysql

# For getting messages according to language of the user
message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('deleteaccount.properties', encoding = "ISO-8859-1")

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
SENDER = environ.get('SENDER')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')

# secret keys for data encryption and security token
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')
key = environ.get('DB_ENCRYPTION_KEY')

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

def make_lambda_client():
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

def send_email2(RECIPIENT, firstname):
    """Function for sending emails """
    # The subject line for the email.
    SUBJECT = (config[message_by_language]['EMAIL_SUBJECT'])

    #get the current day
    day = calendar.day_name[datetime.utcnow().weekday()]

    # The HTML body of the email.
    BODY_HTML = (config[message_by_language]['EMAIL_BODY_TEMPLATE']).format(firstname,day)

    # The character encoding for the email.
    CHARSET = "UTF-8"

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
    logger.info(response)
    return


def handler(event,context):
    """Function to handle the request for Delete Account."""

    global message_by_language
    try:
        logger.info(event)
        # fetching language_id from the event data
        auth_token = event['headers']['Authorization']
    except:
        logger.error(traceback.format_exc())
        # If there is any error in above operations, logging the error
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)

    try:
        # verifying that the user is authorized or not to see this api's data
        rid, user_id, language_id = jwt_verify(auth_token)
    except:
        # if user does not have valid authorization
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['UNAUTHORIZED'], 403)

    try:
        # Making the DB connection
        cnx = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        try:
            # Query for getting current language of the user
            selectionQuery = "SELECT `language_id`, CAST(AES_DECRYPT(`primary_email`,%s) AS CHAR), CAST(AES_DECRYPT(`firstname`,%s) AS CHAR), `ac_contact_id`, `social_userid` FROM `users` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (key, key, rid))

            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)

            logger.info(result_list)

            # getting current language_id of the user 
            language_id = result_list[0][0]
            contact_id = result_list[0][3]
            social_userid = result_list[0][4]
            message_by_language = str(language_id) + "_MESSAGES"

            # Query for getting current user_interpersonal_reports related to the user
            selectionQuery = "SELECT `user_id`, `partner_userid` FROM `user_interpersonal_report` WHERE `rid` = %s OR `partner_id` = %s"
            # Executing the Query
            cursor.execute(selectionQuery, (rid, rid))

            interpersonal_result_list = []
            # fetching result from the cursor
            for result in cursor: interpersonal_result_list.append(result)

            # making an boto 3 client object
            invokeLam = make_lambda_client()

            try:
                # getting the payload for python
                payload = {"Authorization" : auth_token, "interpersonal_result_list" : interpersonal_result_list}
                # invoking the lambda function with custom payload
                response = invokeLam.invoke(FunctionName= "ProfilesDeleteUsersS3Objects" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
            except:
                # If there is any error in above operations, logging the error
                logger.error(traceback.format_exc())
                return log_err(config[message_by_language]['INVOCATION_ERROR'], 500)

            if contact_id != None:
                try:
                    # getting the payload for python
                    payload = {"headers":{"Authorization" : auth_token, "contact_id" : contact_id}}
                    # invoking the lambda function with custom payload
                    response = invokeLam.invoke(FunctionName= "ProfilesDeleteActiveCampaignContact" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
                except:
                    # If there is any error in above operations, logging the error
                    logger.error(traceback.format_exc())
                    return log_err(config[message_by_language]['INVOCATION_ERROR'], 500)


            # Commented the below code due to the removal of Facebook Friends Functionality

            # # checking that if user is connected to facebook or not
            # if social_userid != None:
            #     # deleting all the entry of user inside the users friend list
            #     deletionQuery = "DELETE FROM `user_friends` WHERE `friend_id` = %s"
            #     # Executing the query using cursor
            #     cursor.execute(deletionQuery, (str(social_userid)))


            # deleting all the user_products related to the user
            deletionQuery = "DELETE FROM `user_product` WHERE `id` IN (SELECT DISTINCT(`user_product_id`) FROM `user_partner_products` WHERE `user_rid`=%s OR `partner_rid`=%s)"
            # Executing the query using cursor
            cursor.execute(deletionQuery, (rid, rid))

            # Constructing the query to delete user profile
            query = "Delete from `users` WHERE `id`=%s"
            # Executing the query using cursor
            cursor.execute(query, (rid))

            try:
                # function for sending email
                send_email2(result_list[0][1], result_list[0][2])
            except:
                logger.error(traceback.format_exc())
                # If there is any error in above operations, logging the error
                return log_err (config[message_by_language]['EMAIL_STATUS'], 500)

            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
                'body': json.dumps({"message":config[message_by_language]['SUCCESS_MESSAGE']})
            }
        except:
            logger.error(traceback.format_exc())
            # If there is any error in above operations, logging the error
            return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
    except:
        logger.error(traceback.format_exc())
        # If there is any error in above operations, logging the error
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)
    finally:
        try:
            # Finally, clean up the connection
            cursor.close()
            cnx.close()
        except:
            pass

if __name__== "__main__":
    handler(None,None)
