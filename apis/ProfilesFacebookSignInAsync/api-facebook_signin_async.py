"""API Module to be called asynchronously from ProfilesFacebookSignIn to perform some long operations.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- Fetching user_profile details from event
- deleting friends old friend_list and inserting updated friend list
- Returning the JSON response with success status code

"""

import json
import pymysql
import logging
import traceback
from os import environ
import configparser

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('facebook_signin_async.properties', encoding = "ISO-8859-1")


# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

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
    global message_by_language
    try:
        # fetching user data from event json
        profile = event['profile']
        rid = event['rid']
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
        try:
            # Constructing the query to insert friends list in user_friends table
            deleteQuery = "DELETE FROM `user_friends` where `social_userid`=%s"
            # Executing the batch query
            cursor.execute(deleteQuery, fb_id)
            affected_rows = cursor.rowcount
            logger.info("row count inserting friends" + str(affected_rows))
        except:
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['DELETE_USER_FRIENDS'])
        
        try:
            friends = profile['friends']['data']
            friend_social_userid = tuple([key]) + tuple([i['id'] for i in friends])
            s = ','.join(['%s' for i in friends])
            
            if not s:
                s = str("null")
            result_list = []
            
            try:
                query = "SELECT `id`,`social_userid`,cast(AES_DECRYPT(`name`,%s) as char) FROM `users` WHERE `social_userid` IN (" + s + ")"
                logger.info(query)
                logger.info(friend_social_userid)
                cursor.execute(query, friend_social_userid)
                for result in cursor: result_list.append(result)
            except:
                logger.error(traceback.format_exc())
                return log_err(config[message_by_language]['FRIENDS_INFO'])
            
            friends = tuple([(int(rid), fb_id, i[1]) for i in result_list])
            logger.info(friends)
            
            try:
                # Constructing the query to insert friends list in user_friends table
                insertQuery = "INSERT INTO `user_friends` (`rid`, `social_userid`, `friend_id`) VALUES (%s ,%s, %s)"
                # Executing the batch query
                logger.info(insertQuery)
                logger.info(friends)
                cursor.executemany(insertQuery, friends)
                affected_rows = cursor.rowcount
                logger.info("row count inserting friends" + str(affected_rows))
            except:
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['INSERT_USER_FRIENDS'])
        except:
            logger.info(config[message_by_language]['USER_FRIENDS_DISABLE'])
            pass
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['DATA_PROBLEM'])
        
    # TODO implement
    return {
            'statusCode': 200,
            'body': json.dumps('Hello from Test2Async!')
           }