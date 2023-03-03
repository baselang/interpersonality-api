"""API Module to be called asynchronously from ProfilesFacebookSignup to perform some long operations.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- Fetching user_profile details from event
- Inserting friends in user_friends and updating mystery counter of all friends whose mystery in On
- Returning the JSON response with success status code

"""

import json
import pymysql
import logging
import traceback
from os import environ
import configparser
from datetime import datetime
import boto3

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('facebook_signup_async.properties', encoding = "ISO-8859-1")


# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')
PROFILES_LINK = environ.get('PROFILES_LINK')

# Variables related to s3 bucket
AWS_REGION =  environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')

# Getting key for getting token
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

def log_err(errmsg):
    """Function to log the error messages."""
    return  {
                "statusCode": 400,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def handler(event, context):
    logger.info(event)
    try:
        # fetching user data from event json
        profile = event['profile']
        rid = event['rid']
        user_id = event['user_id']
        fb_id = profile['id']
        name = profile['name']
        firstname = profile['first_name']
        lastname = profile['last_name']
        picture_url = profile['picture']['data']['url']
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'])
        
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'])
        
    try:
        # creating a boto3 service client object
        invokeLam = boto3.client(
                                    "lambda", 
                                    region_name=AWS_REGION,
                                    aws_access_key_id=AWS_ACCESS_KEY,
                                    aws_secret_access_key=AWS_SECRET
                                )
    except:
        # when there is some problem in above code
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['BOTO_SERVICE_CLIENT_STATUS'], 500)
    
    try:
        try:
            # getting the is_active field from the users table to check that the test is complete or not
            selectionQuery = "SELECT `is_active` FROM `users` WHERE `id`=%s"
            # Executing the query
            cursor.execute(selectionQuery, (rid))
            user_data = []
            # getting the result list from the cursor
            for result in cursor: user_data.append(result)
            # getting the value of is_active field from the list
            is_active = user_data[0][0]
        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['IS_ACTIVE_STATUS'])
                
        if is_active == 1:
            try:
                # preparing the payload for requesting to a lambda function
                payload = {'rid':int(rid)}
                # invoking a lambda function call
                invokeLam.invoke(FunctionName="ProfilesSendNotificationsToFriendsAsync" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
            except:
                # when there is some problem in above code
                logger.error(traceback.format_exc())
                return log_err(config[message_by_language]['INVOKING_ASYNC_STATUS'])
        
        # Commented the below code due to the removal of Facebook Friends Functionality
        #try:
        #    friends = profile['friends']['data']
        #        
        #    # preparing input for the below query
        #    s = ','.join(['%s' for i in friends])
        #    my_friends = tuple([i['id'] for i in friends])
        #    friend_social_userid = tuple([key]) + my_friends
        #   friend_details = friend_social_userid + my_friends
        #    
        #   result_list = []
        #    
        #    if friends == []:
        #        # if user has no friends and he has completely signed up than sending
        #        
        #        try:
        #            # getting the is_active field from the users table to check that the test is complete or not
        #            selectionQuery = "SELECT `is_active` FROM `users` WHERE `id`=%s"
        #            # Executing the query
        #            cursor.execute(selectionQuery, (rid))
        #            user_data = []
        #            # getting the result list from the cursor
        #            for result in cursor: user_data.append(result)
        #            # getting the value of is_active field from the list
        #            is_active = user_data[0][0]
        #        except:
        #            logger.error(traceback.format_exc())
        #            return log_err(config[message_by_language]['IS_ACTIVE_STATUS'])
        #        
        #        if is_active == 1:
        #            try:
        #                # preparing the payload for requesting to a lambda function
        #                payload = {'rid':int(rid)}
        #                # invoking a lambda function call
        #                invokeLam.invoke(FunctionName="ProfilesSendNotificationsToFriendsAsync" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
        #            except:
        #                # when there is some problem in above code
        #                logger.error(traceback.format_exc())
        #                return log_err(config[message_by_language]['INVOKING_ASYNC_STATUS'])
        #    else:
        #        try:
        #            # Query for getting friends details of a user
        #            query = "SELECT `id`,`social_userid`,cast(AES_DECRYPT(`name`,%s) as char), `user_id` FROM `users` WHERE `social_userid` IN (" + s + ")"
        #            # Executing the query
        #            cursor.execute(query, friend_social_userid)
        #            result_list = []
        #            for result in cursor: result_list.append(result)
        #        except:
        #            logger.error(traceback.format_exc())
        #            return log_err(config[message_by_language]['FRIENDS_INFO'])
        #        
        #        # preparing input for the below query
        #        friends = tuple([(int(rid), fb_id, i[1]) for i in result_list]) + tuple([(int(i[0]), i[1], fb_id)  for i in result_list])
        #        
        #        try:
        #            # Constructing the query to insert friends list in user_friends table
        #            insertQuery = "INSERT INTO `user_friends` (`rid`, `social_userid`, `friend_id`) VALUES (%s ,%s, %s)"
        #            # Executing the batch query
        #            cursor.executemany(insertQuery, friends)
        #            affected_rows = cursor.rowcount
        #            logger.info("row count inserting friends" + str(affected_rows))
        #        except:
        #            logger.error(traceback.format_exc())
        #            return log_err (config[message_by_language]['INSERT_USER_FRIENDS'])
        #            
        #        try:
        #            # getting the is_active field from the users table to check that the test is complete or not
        #            selectionQuery = "SELECT `is_active` FROM `users` WHERE `id`=%s"
        #            # Executing the query
        #            cursor.execute(selectionQuery, (rid))
        #            user_data = []
        #            # getting the result list from the cursor
        #            for result in cursor: user_data.append(result)
        #            # getting the value of is_active field from the list
        #            is_active = user_data[0][0]
        #        except:
        #            logger.error(traceback.format_exc())
        #            return log_err(config[message_by_language]['IS_ACTIVE_STATUS'])
        #            
        #        if is_active == 1:
        #            try:
        #               payload = {'rid':int(rid)}
        #                invokeLam.invoke(FunctionName="ProfilesSendNotificationsToFriendsAsync" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
        #            except:
        #                # when there is some problem in above code
        #                logger.error(traceback.format_exc())
        #                return log_err (config[message_by_language]['INVOKING_ASYNC_STATUS'])
        #except KeyError:
        #    logger.info(config[message_by_language]['USER_FRIENDS_DISABLE'])
        #    pass            
        # TODO implement
        return  {
                    'statusCode': 200,
                    'body': json.dumps('Hello from Test2Async!')
                }
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['DATA_PROBLEM'])