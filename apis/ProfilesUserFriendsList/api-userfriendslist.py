"""API Module to get user friend details.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. make_client(): To make a boto3 client and S3 object for invoking lambda function
5. handler(): Handling the incoming request with following steps:
- Getting required data from user
- Getting data of user friends from the application
- Returning the JSON response with the requested data for user facebook friends and success status code

"""
import json
import pymysql
import logging
import traceback
from os import environ
import configparser
import jwt
import boto3

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('userfriendslist.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# Getting key for getting token
key = environ.get('DB_ENCRYPTION_KEY')

# Getting key for verification of token
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

# aws cridentials required for creating boto3 client object
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')

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
    """Function to handle the request for Get Big5 API."""
    logger.info(event)
    global message_by_language
    
    try:
        logger.info(event)
        # checking that the following event call is from lambda warmer or not
        if event['source']=="lambda_warmer":
            logger.info("lambda warmed")
            # returning the success json
            return {
                       'status_code':200,
                       'body':{"message":"lambda warmed"}
                   }
    except:
        # If there is any error in above operations
        pass

    try:
        # fetching data from event request object
        auth_token = event['headers']['Authorization']
        # try:
        #     # making an boto 3 client object
        #     invokeLam = make_client()
        #
        #     # preparing the payload for lambda invocation
        #     payload = {"headers": {"Authorization": auth_token}}
        #
        #     # invoking the lambda function with custom payload
        #     response = invokeLam.invoke(FunctionName="ProfilesRegenerateFacebookImages" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
        #
        # except:
        #     # If there is any error in above operations, logging the error
        #     logger.error(traceback.format_exc())
        #     return log_err(config[message_by_language]['INVOCATION_ERROR'])
        #
    except:
        logger.error(traceback.format_exc())
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
    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)
    
    try:
        # Query for getting current language of the user
        selectionQuery = "SELECT `language_id`, `social_userid`,`language_id`,`is_visited_friends`,`picture_url` FROM `users` WHERE `id`=%s"
        # Executing the Query
        cursor.execute(selectionQuery, (rid))
        
        result_list = []
        # fetching result from the cursor
        for result in cursor: result_list.append(result)
        
        # getting current language_id of the user 
        language_id = result_list[0][0]

        # Getting details of the user out from the executed query
        social_userid = result_list[0][1]
        language_id = result_list[0][2]

        # getting user is visiting friends page first time or not to show privacy setting
        is_visited_friends = int(result_list[0][3])
        picture_url = result_list[0][4]
        message_by_language = str(language_id) + "_MESSAGES"
    except:
        # If there is any error in above operations, logging the error
        return log_err (config[message_by_language]['QUERY_STATUS'], 500)

    flag = 1

    try:
        logger.info("start friend")
        friend_userid = event['headers']['friend_id']
        
        logger.info(friend_userid)
        if friend_userid != user_id:
            # response when user is logged in and accessing own profile
            # Query to get facebook id associated with the user
            selectionQuery = "SELECT CASE WHEN (SELECT `social_userid` from `users` where `user_id`=%s) IN (SELECT `friend_id` FROM `user_friends` WHERE `rid`=%s) THEN 1 ELSE 0 END AS `is_friend`"
            # Executing the query
            cursor.execute(selectionQuery, (friend_userid, rid))
            result_list = []
            for result in cursor: result_list.append(result)
            logger.info(result_list)
            flag = result_list[0][0]
        else:
            flag = 1
    except:
        friend_userid = None



    if flag==0:
        return log_err(config[message_by_language]['IS_FRIEND_STATUS'], 400)
    elif flag==1:
        try:
            # # Query to get facebook id associated with the user
            # selectionQuery = "SELECT `social_userid`,`language_id`,`is_visited_friends`,`picture_url` FROM `users` WHERE `id`=%s"
            # # Executing the query
            # cursor.execute(selectionQuery, (rid))
            # result_list = []
            # for result in cursor: result_list.append(result)
            # logger.info(result_list)
            #
            # # Getting details of the user out from the executed query
            # social_userid = result_list[0][0]
            # language_id = result_list[0][1]
            # # getting user is visiting friends page first time or not to show privacy setting
            # is_visited_friends = int(result_list[0][2])
            # picture_url = result_list[0][3]

            if is_visited_friends == 0 and social_userid!=None:
                # when first time he is visiting than updating the visiting field since user is already visiting the friends page
                try:
                    # 
                    updateQuery = "UPDATE `users` SET `is_visited_friends`=1 WHERE `id`=%s"
                    cursor.execute(updateQuery, (rid))
                except:
                    logger.error(traceback.format_exc())
                    return log_err(config[message_by_language]['QUERY_STATUS'], 500)
                    
            message_by_language = str(language_id) + '_MESSAGES'
            
            # checking if user is linked to any facebook account or not and returning response according to it
            if social_userid==None:
                return  {
                        'statusCode': 200,
                        'headers': {
                                    'Access-Control-Allow-Origin': '*',
                                    'Access-Control-Allow-Credentials': 'true'
                                   },
                        'body': json.dumps({'auth':auth_token, 'user_id':user_id, 'is_connected':'false'})
                        }
        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['QUERY_STATUS'], 500)

        try:
            # Query to get user friends details from the database 
            selectionQuery = "SELECT `user_id`,cast(AES_DECRYPT(`name`,%s) as char),`picture_url` FROM `users` WHERE `privacy_settings_status` in (1,2) AND `social_userid` IN (SELECT `friend_id` FROM `user_friends` WHERE `social_userid`=%s) AND `is_active`=1"
            # Executing the query
            cursor.execute(selectionQuery, (key, social_userid))
            
            try:
                if friend_userid != None:
                    # if user is accessing other friends profile
                    friends_list = []
                    
                    for friend in cursor:
                        if friend[0] == friend_userid:
                            # Adding the friend whose profile is active at the beginning and setting its class to active
                            friends_list.insert(0, {"friend_userid":friend[0],"friend_name":friend[1],"friend_picture_url":friend[2], "class":"active"})
                        else:
                            # Adding the friends to the friends list
                            friends_list.append({"friend_userid":friend[0],"friend_name":friend[1],"friend_picture_url":friend[2]})
                else:
                    # if user is accessing api on his own friends page
                    friends_list = [{"friend_userid":friend[0],"friend_name":friend[1],"friend_picture_url":friend[2]} for friend in cursor]
            except:
                logger.error(traceback.format_exc())
                # returning the json response if a user has no friends
                return  {
                        'statusCode': 200,
                        'headers': {
                                    'Access-Control-Allow-Origin': '*',
                                    'Access-Control-Allow-Credentials': 'true'
                                   },
                        'body': json.dumps({'auth':auth_token, 'user_id':user_id, 'is_connected':'true', 'friends_list':[],'is_visited_friends':is_visited_friends})
                    }

            if friends_list == []:
                return  {
                            'statusCode': 200,
                            'headers': {
                                        'Access-Control-Allow-Origin': '*',
                                        'Access-Control-Allow-Credentials': 'true'
                                       },
                            'body': json.dumps({'auth':auth_token, 'user_id':user_id, 'is_connected':'true', 'friends_list':[],'is_visited_friends':is_visited_friends})
                        }
            else :
                # returning the json response if a user has friends
                return  {
                            'statusCode': 200,
                            'headers': {
                                        'Access-Control-Allow-Origin': '*',
                                        'Access-Control-Allow-Credentials': 'true'
                                       },
                            'body': json.dumps({'auth':auth_token, 'user_id':user_id, 'is_connected':'true', 'friends_list':friends_list,'is_visited_friends':is_visited_friends, 'picture_url':picture_url})
                    }
        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['QUERY_STATUS'], 500)
    
