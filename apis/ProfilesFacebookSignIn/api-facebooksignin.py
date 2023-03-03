"""API Module to Login to our application by using facebook login.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- Fetching facebook code from event
- Fetching faebook details of user using code
- Checking if user has signed up to our Application or not and if not returining json response with required error message
- generating an access token so that user can verified after login
- Returning the JSON response with success status code with the authentication token and user_id in the response body

"""

import json
import pymysql
import logging
import traceback
from os import environ
import urllib
import requests
import facebook
import jwt
from datetime import datetime 
from datetime import timedelta
import configparser
import boto3
import uuid

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('facebooksignin.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
app_id = environ.get('APP_ID')
app_secret = environ.get('APP_SECRET')
redirect_url_fb = environ.get('REDIRECT_FB_URL')

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')
PROFILES_LINK = environ.get('PROFILES_LINK')

# Getting key for getting token
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')
key = environ.get('DB_ENCRYPTION_KEY')
TOKEN_EXPIRY_TIME = environ.get('TOKEN_EXPIRY_TIME')

# aws cridentials required for creating boto3 client
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')
IMAGE_SIZE = environ.get('IMAGE_SIZE')

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
                "statusCode": 500,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def handler(event,context):
    """Function to handle the request for facebooksignin."""
    global message_by_language
    global TOKEN_EXPIRY_TIME
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
        # Fetching event data from request event object
        code = event['headers']['code']
        language_id = event['headers']['language_id']
        ip = event['requestContext']['identity']['sourceIp']
        try:
            referral_code = event['headers']['referral_code']
            if referral_code != "null":
                referral_code = "\"" + referral_code + "\""
        except:
            logger.error(traceback.format_exc())
            referral_code = "null"
    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'])
        
    try:
        # creating a boto3 service client object
        invokeLam = boto3.client(
                                "lambda", 
                                region_name=AWS_REGION,
                                aws_access_key_id=AWS_ACCESS_KEY,
                                aws_secret_access_key=AWS_SECRET
                                )
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['BOTO_SERVICE_CLIENT_STATUS'])
        
        
    try:
        # Fetching user data from facebook by getting access_token using code
        my_url = "https://graph.facebook.com/oauth/access_token?"+"client_id=" + app_id +"&redirect_uri="+ urllib.parse.quote(redirect_url_fb) +"&client_secret=" + app_secret +"&code=" + code
        r = requests.get(url=my_url)
        logger.info(r.json())
        acc_token = r.json()['access_token']
        
        # Fetching user data from facebook by using acess_token
        graph = facebook.GraphAPI(acc_token)
        profile = graph.get_object('me', fields ='name, email, picture.width(' + str(IMAGE_SIZE) + ').height(' + str(IMAGE_SIZE) + '), first_name, last_name')
        logger.info(profile)
        name = profile['name']
        firstname = profile['first_name']
        lastname = profile['last_name']
        picture_url = profile['picture']['data']['url']
        fb_id = profile['id']
        is_email_exist = True
        is_email = True
        test_status = 'completed'
        
        # checking email is present or not
        try:
            email = profile['email'].lower()
        except:
            is_email = False
        user_id = 0
        
        try:
            # Making the DB connection
            cnx    = make_connection()
            # Getting the cursor from the DB connection to execute the queries
            cursor = cnx.cursor()
        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['CONNECTION_STATUS'])
            
        
        try:
            try:
                # selection query for checking that user with particular facebook id exist in our application database or not
                selectionQuery = "SELECT `id`,`user_id`,cast(AES_DECRYPT(`firstname`,%s) as char), cast(AES_DECRYPT(`name`,%s) as char),`language_id` FROM `users` WHERE `social_userid`=%s"
                cursor.execute(selectionQuery, (key,key, str(fb_id)))
                result_list = []
                
                # fetching data we got after execution of above query
                for result in cursor: result_list.append(result)
                logger.info(result_list)
                rid = result_list[0][0]
                user_id = result_list[0][1]
                firstname1 = result_list[0][2]
                name = result_list[0][3]
                language_id = result_list[0][4]
            except:
                # checking email associated with fb account or not 
                if is_email == False:
                    message_by_language = str(language_id) + "_MESSAGES"
                    return {
                        'statusCode': 400,
                        'headers': {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                        },
                        'body': json.dumps({"message": config[message_by_language]['FACEBOOK_EMAIL_EXISTENCE_STATUS']})
                    }
                
                # if user is not present in our application than signing up the user
                try:
                    # generating user_id if user has not signed up to Profiles application
                    _uuid = str(uuid.uuid4())
                    
                    # for creating new user setting variables if user is not already exist in database
                    user_id = _uuid
                    test_status = 'not_completed'
                    gender = 0
                    
                    try:
                        # Query for checking that email associated with the provided email id by facebook exist or not
                        query = "SELECT `rid`, `user_id` FROM `user_emails` WHERE `email`=AES_ENCRYPT(%s,%s)"
                        # Executing the query
                        cursor.execute(query, (email,key))
                        result_list = []
                        # Fetching result from user
                        for result in cursor: result_list.append(result)
                        # checking that email id exist or not
                        rid = result_list[0][0]
                        user_id = result_list[0][1]

                        query1 = "SELECT cast(AES_DECRYPT(`firstname`,%s) as char) FROM `users` WHERE id=%s"
                        cursor.execute(query1, (key, rid))
                        result_list = []
                        for result in cursor: result_list.append(result)
                        firstname1 = result_list[0][0]

                        # returning the message with failed status
                        is_email_exist = False
                    except:
                        is_email_exist = True
                    
                    if is_email_exist:
                        
                        if referral_code != "null":
                            # checking that a referral user exist or not
                            selectionQuery = "SELECT `user_id` FROM `users` WHERE `user_id`=" + referral_code
                            cursor.execute(selectionQuery)
                            referral_user_exist = []
                            for ref_result in cursor: referral_user_exist.append(ref_result)
                            
                            if len(referral_user_exist) == 0:
                                referral_code = "null"
                        
                        try:
                            # Inserting or updating the user details into the users table
                            query = "INSERT INTO `users` (`user_id`, `ip`, `social_userid`, `primary_email`, `firstname`, `lastname`, `name`, `picture_url`, `gender`, `referral_code`, `language_id`) VALUES (%s, %s, %s, AES_ENCRYPT(%s, %s), AES_ENCRYPT(%s, %s), AES_ENCRYPT(%s, %s), AES_ENCRYPT(%s, %s), %s, %s," + referral_code + ", %s)"
                            # Executing the query
                            cursor.execute(query, (_uuid, ip, str(fb_id), email, key, firstname, key, lastname, key, name, key, picture_url, gender, int(language_id)))
                        except:
                            logger.error(traceback.format_exc())
                            return log_err(config[message_by_language]['INSERT_USER_STATUS'])
                            
                        try:
                            # Inserting or updating the user details into the users table
                            query = "SELECT `id` FROM `users` WHERE `user_id`=%s"
                            # Executing the query
                            cursor.execute(query, (_uuid))
                            result_list = []
                            # fetching data we got after execution of above query
                            for result in cursor: result_list.append(result)
                            rid = int(result_list[0][0])
                        except:
                            logger.error(traceback.format_exc())
                            return log_err(config[message_by_language]['RID_STATUS'])
                            
                        try:
                            # Constructing the query to insert email of an user into user_emails table
                            insertQuery = "INSERT INTO `user_emails` (`rid`, `user_id`, `email`) VALUES (%s, %s ,AES_ENCRYPT(%s, %s))"
                            # Executing the query using
                            cursor.execute(insertQuery, (rid, _uuid, email, key))
                        except:
                            logger.error(traceback.format_exc())
                            return log_err (config[message_by_language]['INSERT_USER_EMAIL_STATUS'])
                        
                        # Commented the below code due to the remove of Facebook Friends Functionality    
                        try:
                            # creating payload to call lambda function asynchronously
                            payload = {'profile':profile, 'rid':rid, 'user_id':user_id, "referral_code":referral_code}
                            # calling facebooksignup asynchronous function to insert friend list of user
                            invokeLam.invoke(FunctionName="ProfilesFacebookSignUpAsync" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
                        except:
                            logger.error(traceback.format_exc())
                            return log_err (config[message_by_language]['INVOKING_ASYNC_STATUS'])
                except:
                    logger.error(traceback.format_exc())
                    return log_err(config[message_by_language]['USER_EXISTENCE'])
                    
            try:
                # adding additional data like picture_url and social_userid into the old user
                query = "UPDATE `users` SET `language_id` = %s, `picture_url` = CASE WHEN `picture_url` IS NULL THEN %s WHEN `is_picture_uploaded` = 0 THEN %s ELSE `picture_url` END, `social_userid` = CASE WHEN `social_userid` IS NULL THEN %s ELSE `social_userid` END WHERE `id` = %s"
                # Executing the query
                cursor.execute(query, (language_id, picture_url, picture_url, str(fb_id), int(rid)))

            except:
                logger.error(traceback.format_exc())
                return log_err(config[message_by_language]['UPDATE_USER'])
                
            # creating a payload for generating authentication token
            payload = {}
            payload['id'] = rid
            payload['name'] = name
            payload['language_id'] = language_id
            payload['user_id'] = user_id
            payload['exp'] = datetime.timestamp(datetime.now() + timedelta(days=int(TOKEN_EXPIRY_TIME)))
            try:
                # generating an authentication token of a user
                token =  jwt.encode(payload, SECRET_KEY)
                logger.info("token: " + str(token)[2:-1])
            except:
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['TOKEN_STATUS'])
                
            try:
                # selection query for getting test_status from the user_permission when
                selectionQuery = "SELECT CASE WHEN COUNT(*) > 0 AND `is_active`=1 THEN \"completed\" ELSE \"not_completed\" END FROM `users` WHERE `id`=%s"
                # Executing the Query
                cursor.execute(selectionQuery, (int(rid)))
                
                result_list = []
                # fetching data we got after execution of above query
                for result in cursor: result_list.append(result)
                
                # getting the test_status from the result_list
                test_status = result_list[0][0]
                
                if test_status == "not_completed":
                    # when user has answered all 120 questions
                    try:
                        # Query for deleting user responses when test is not fully completed i.e. till is_active field is not set to 1
                        deletionQuery = "DELETE FROM `user_responses` WHERE `rid`=%s AND (SELECT `is_active` FROM `users` WHERE `id`=%s)!=1"
                        # Executing the query
                        cursor.execute(deletionQuery, (int(rid), int(rid)))
                        
                        # Query for deleting description responses (output_responses) when test is not fully completed i.e. till is_active field is not set to 1
                        deletionQuery = "DELETE FROM `output_responses` WHERE `rid`=%s AND (SELECT `is_active` FROM `users` WHERE `id`=%s)!=1"
                        # Executing the query
                        cursor.execute(deletionQuery, (int(rid), int(rid)))
                    except:
                        logger.error(traceback.format_exc())
                        return log_err(config[message_by_language]['DELETING_RESPONSES'])
            except:
                logger.error(traceback.format_exc())
                return log_err(config[message_by_language]['TEST_STATUS'])
            
            # Commented the below code due to the remove of Facebook Friends Functionality
            #if test_status == 'completed':
            #    # calling facebooksignin asynchronous partition
            #    try:
            #        # creating payload to call lambda function asynchronously
            #        payload = {'profile':profile,'rid':rid}
            #        # invoking lambda function asynchronously
            #        invokeLam.invoke(FunctionName="ProfilesFacebookSignInAsync" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
            #    except:
            #        logger.error(traceback.format_exc())
            #        return log_err (config[message_by_language]['INVOKING_ASYNC_STATUS'])
            
            if test_status == 'completed':
                # returning the json response with success code and required data when user has completed test
                return  {
                        'statusCode': 200,
                        'headers': {
                                    'Access-Control-Allow-Origin': '*',
                                    'Access-Control-Allow-Credentials': 'true'
                                   },
                        'body': json.dumps({'auth':token.decode('utf-8'), 'user_id':user_id, 'rid':rid, 'test_status':test_status, 'language_id':language_id, 'firstname':firstname1})
                    }
            else:
                # returning the json response with success code and required data when user has not completed his test
                return  {
                        'statusCode': 200,
                        'headers':{
                                    'Access-Control-Allow-Origin': '*',
                                    'Access-Control-Allow-Credentials': 'true'
                                   },
                        'body': json.dumps({'auth':token.decode('utf-8'), 'user_id':user_id, 'rid':rid, 'test_status':test_status, 'firstname':firstname, 'language_id':language_id})
                        }
        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['RESPONSE_STATUS'])
    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['FACEBOOK_DATA_STATUS'])

if __name__== "__main__":
    handler(None,None)