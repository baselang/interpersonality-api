"""API For updating user basic info.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. make_client(): To make a boto3 client and S3 object for invoking lambda function
5. handler(): Handling the incoming request with following steps:
- Fetching data from request
- updating basic information of user
- inserting new emails
- Returning the JSON response with success status code with the message ,authentication token and user_id in the response body
"""
import botocore
import jwt
import json
import pymysql
import logging
import traceback
from os import environ
import configparser
import boto3
from botocore.client import Config
import base64
import chargebee
import time

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('updateuserbasicinfo.properties', encoding = "ISO-8859-1")

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
    return {
        "statusCode": status_code,
        "body": json.dumps({"message": errmsg}),
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': 'true'},
        "isBase64Encoded": "false"
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


def handler(event, context):
    """Function to handle the request for update user basic info API"""
    global message_by_language
    logger.info(event)
    try:
        # Fetching data from event and rendering it

        auth_token = event['headers']['Authorization']

        body = json.loads(event['body'])

        firstname = body['firstname']
        lastname = body['lastname']
        language_id = int(body['language_id'])

        try:
            emails = body['emails']
            if len(emails) == 0:
                emails = 0
            else:
                emails = (emails)
        except:
            emails = 0
        try:
            password = body['password']
            if password == "":
                password = 0
        except:
            password = 0

        name = firstname + " " + lastname
    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)

    try:
        # verifying that the user is authorized or not to see this api's data
        rid, user_id, old_language_id = jwt_verify(auth_token)
    except:
        # if user does not have valid authorization
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['UNAUTHORIZED'], 403)

    try:
        # Making the DB connection
        cnx = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)

    try:
        # Query for getting current language of the user
        selectionQuery = "SELECT `language_id`, `ac_contact_id`, CAST(AES_DECRYPT(`firstname`, %s) AS char),CAST(AES_DECRYPT(`lastname`, %s) AS char) FROM `users` WHERE `id`=%s"
        # Executing the Query
        cursor.execute(selectionQuery, (key, key, rid))

        result_list = []
        # fetching result from the cursor
        for result in cursor: result_list.append(result)

        # getting current language_id of the user
        old_language_id = result_list[0][0]
        ac_contact_id = result_list[0][1]
        old_firstname = result_list[0][2]
        old_lastname = result_list[0][3]
        message_by_language = str(old_language_id) + "_MESSAGES"
    except:
        # If there is any error in above operations, logging the error
        return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)

    result_list = []
    try:
        if emails != 0:
            # Remove duplicates email from list
            emails = list(dict.fromkeys([email.lower() for email in emails]))
            # generating string for inserting emails
            s = ",".join(["AES_ENCRYPT(\"" + i + "\",\"" + key + "\")" for i in emails])

            # Query for getting emails related to the user
            selectionQuery = "SELECT CAST(AES_DECRYPT(`email`, %s ) AS CHAR) FROM `user_emails` WHERE `email` IN (" + s + ")"
            # Executing the Query or cursor
            cursor.execute(selectionQuery, (key))

            # getting data from cursor from above query
            for result in cursor: result_list.append(result[0])
            # generating emails list which are valid to get inserted
            emails_inserted = tuple([(rid, user_id, j, key) for j in filter(lambda i: i not in result_list, emails)])

            if len(emails_inserted) == 0:
                return {'statusCode': 400,
                        'headers': {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                        },
                        'body': json.dumps({"message": config[message_by_language]['EMAIL_ID_STATUS']})
                        }

            # Query for inserting emails into user_emails table in database
            insertionQuery = "INSERT INTO `user_emails` (`rid`, `user_id`, `email`) VALUES (%s, %s, AES_ENCRYPT(%s,%s))"
            # Executing the Query or cursor
            cursor.executemany(insertionQuery, emails_inserted)

        if password != 0:

            # Query to update user details
            updationQuery = "UPDATE `users` SET `name`=AES_ENCRYPT(%s,%s),`firstname`=AES_ENCRYPT(%s,%s), `lastname`=AES_ENCRYPT(%s,%s), `password`=AES_ENCRYPT(%s,%s), `language_id`=%s WHERE `id`=%s"
            # Executing the Query or cursor
            cursor.execute(updationQuery, (name, key, firstname, key, lastname, key, password, key, language_id, rid))
        else:
            # Query to update user details
            updationQuery = "UPDATE `users` SET `name`=AES_ENCRYPT(%s,%s),`firstname`=AES_ENCRYPT(%s,%s), `lastname`=AES_ENCRYPT(%s,%s), `language_id`=%s WHERE `id`=%s"
            # Executing the Query or cursor
            cursor.execute(updationQuery, (name, key, firstname, key, lastname, key, language_id, rid))

        message_by_language = str(language_id) + "_MESSAGES"

        if len(result_list) <= 0:
            # if no invalid emails are present in request
            message = config[message_by_language]['PROFILE_UPDATE_SUCCESSFULLY']
        else:
            # if invalid emails are present in email list in request
            message = (config[message_by_language]['PROFILE_UPDATE_SUCCESS_BUT_EMAIL_EXIST'])
    
        try:
            # making an boto 3 client object
            invokeLam = make_client()

        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['INVOCATION_ERROR'], 500)

        try:
            if [int(old_language_id), old_firstname, old_lastname] != [int(language_id), firstname, lastname]:

                try:
                    # configuring chargebee object
                    chargebee.configure(SITE_KEY, SITE_URL)
                    # Updating Chargebee customer locale language
                    selectionQuery = "SELECT `language_id`, `customer_id`, CAST(AES_DECRYPT(`firstname`,%s) AS CHAR), CAST(AES_DECRYPT(`lastname`,%s) AS CHAR), CAST(AES_DECRYPT(`primary_email`,%s) AS CHAR) FROM `users` WHERE `id` = %s"
                    # Executing the Query
                    cursor.execute(selectionQuery, (key, key, key, int(rid)))

                    result_list = []
                    # fetching result from the cursor
                    for result in cursor: result_list.append(result)

                    # getting the data from the result_list
                    language_id = result_list[0][0]
                    customer_id = result_list[0][1]
                    first_name = result_list[0][2]
                    last_name = result_list[0][3]
                    email = result_list[0][4]
                    if customer_id !=None:

                        selectionQuery = "SELECT `code` FROM `language` WHERE `id`=%s"
                        cursor.execute(selectionQuery, (int(language_id)))
                        result_list = []
                        # fetching result from the cursor
                        for result in cursor: result_list.append(result)
                        language_code = result_list[0][0]
                        result = chargebee.Customer.update(customer_id, {
                            "first_name": first_name,
                            "last_name": last_name,
                            "email": email,
                            "locale": language_code,
                        })

                except:
                    logger.error(traceback.format_exc())
                        # preparing payload for the lambda call
                payload = {'rid': int(rid)}

                # calling the lambda function asynchronously to generate an generic profile image for sharing on facebook
                invokeLam.invoke(FunctionName="ProfilesFacebookScrapeImage" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))

                    
            if ac_contact_id != None:
                # preparing the payload for lambda invocation
                payload = {"headers":{"Authorization":auth_token, "user_partner_id": None}}

                invokeLam.invoke(FunctionName= "ProfilesActiveCampaignUpdate" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))

        except:
            # when there is some problem in above code
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['INVOKING_ASYNC_STATUS'], 500)

        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'
            },
            'body': json.dumps({"message": message, "language_id": language_id})
        }
    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
