"""API Module to make user sign up using facebook to our application.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- Fetching facebook code from event
- Fetching faebook details of user using code
- Inserting user data into database to create a new user
- generating an access token so that user can be verified after signup and begin his session
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

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('facebooksignup.properties', encoding = "ISO-8859-1")

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

# aws cridentials required for creating boto3 client object
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')
IMAGE_SIZE = environ.get('IMAGE_SIZE')

# Getting key for getting token
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')
key = environ.get('DB_ENCRYPTION_KEY')
TOKEN_EXPIRY_TIME = environ.get('TOKEN_EXPIRY_TIME')

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
    """Function to handle the request for facebooksign API."""
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
        data = json.loads(event['body'])
        rid = data['rid']
        language_id = event['headers']['language_id']
        message_by_language = str(language_id) + "_MESSAGES"
        try:
            referral_code = event['headers']['referral_code']
            #referral_code = "8651a60f-7c09-419f-a7b3-7765f061aad3"
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
        fb_id = profile['id']
        name = profile['name']
        firstname = profile['first_name']
        lastname = profile['last_name']
        picture_url = profile['picture']['data']['url']
        is_email_exist = True
        
        # checking email is present or not
        is_email = True
        try:
            email = profile['email'].lower()
        except:
            return {
                'statusCode': 400,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
                'body': json.dumps({"message": config[message_by_language]['EMAIL_EXISTENCE']})
            }
        user_id = 0
        
        try:
            # Making the DB connection
            cnx    = make_connection()
            # Getting the cursor from the DB connection to execute the queries
            cursor = cnx.cursor()
            if is_email:
                try:
                    # checking that facebook account already exists or not
                    selectionQuery = "SELECT `id`,`user_id` FROM `users` WHERE `social_userid`=%s"
                    # Executing the query
                    cursor.execute(selectionQuery, (fb_id))
                    result_list = []
                    for result in cursor: result_list.append(result)
                    old_rid = result_list[0][0]
                    old_userid = result_list[0][1]
                    is_already_exist = True
                    logger.info(config[message_by_language]['DUPLICATE_FACEBOOK'])
                except:
                    is_already_exist = False
                    
                
                try:
                    # checking that email associated with the provided email id by facebook exist or not
                    query = "SELECT `rid`,`user_id` FROM `user_emails` WHERE `email`=AES_ENCRYPT(%s,%s)"
                    # Executing the query
                    cursor.execute(query, (email,key))
                    result_list = []
                    for result in cursor: result_list.append(result)
                    result_list[0][0]
                    if is_already_exist==False:
                        old_rid = result_list[0][0]
                        old_userid = result_list[0][1]
                        is_email_exist = False
                        is_already_exist = True
                    else:
                        is_email_exist = False
                except:
                    is_email_exist = True
                try:
                    # getting user_id of account by using provided rid or id of the user
                    query = "SELECT `user_id`,`language_id` FROM `users` WHERE `id`=%s"
                    # Executing the query
                    cursor.execute(query, (rid))
                    result_list = []
                    for result in cursor: result_list.append(result)
                    user_id = result_list[0][0]
                    language_id = result_list[0][1]
                except:
                    logger.error(traceback.format_exc())
                    return log_err(config[message_by_language]['USER_ID_STATUS'])
                
                if referral_code != "null":
                    # checking that a referral user exist or not
                    selectionQuery = "SELECT `user_id` FROM `users` WHERE `user_id`=" + referral_code
                    cursor.execute(selectionQuery)
                    referral_user_exist = []
                    for ref_result in cursor: referral_user_exist.append(ref_result)
                    
                    if len(referral_user_exist) == 0:
                        referral_code = "null"
                    
                if is_already_exist:
                    try:
                        # updating language_id of old user with new users language_id
                        query = "UPDATE `users` SET `language_id` = %s, `picture_url`=CASE WHEN `picture_url` IS NULL THEN %s WHEN `is_picture_uploaded` = 0 THEN %s ELSE `picture_url` END, `social_userid` = %s  WHERE `id` = %s"
                        # Executing the query
                        cursor.execute(query, (language_id, picture_url, picture_url, str(fb_id), old_rid))
                    except:
                        logger.error(traceback.format_exc())
                        return log_err(config[message_by_language]['UPDATE_USER_LANGUAGE'])
                
                try:
                    if is_already_exist:
                        # Inserting or updating the user details into the users table
                        query = "UPDATE `users` SET `social_userid`=%s, `firstname`=AES_ENCRYPT(%s,%s), `lastname`=AES_ENCRYPT(%s,%s), `name`=AES_ENCRYPT(%s,%s), `picture_url` = CASE WHEN `picture_url` IS NULL THEN %s WHEN `is_picture_uploaded` = 0 THEN %s ELSE `picture_url` END WHERE `id` = %s"
                        # Executing the query
                        cursor.execute(query, (str(fb_id), firstname, key, lastname, key, name, key, picture_url, picture_url, int(rid)))
                    else:
                        # Inserting or updating the user details into the users table
                        query = "UPDATE `users` SET `primary_email`=AES_ENCRYPT(%s,%s), `social_userid`=%s, `firstname`=AES_ENCRYPT(%s,%s), `lastname`=AES_ENCRYPT(%s,%s), `name`=AES_ENCRYPT(%s,%s), `picture_url`=%s, `referral_code`=" + referral_code + " WHERE `id`=%s"
                        # Executing the query
                        cursor.execute(query, (email, key, str(fb_id), firstname, key, lastname, key, name, key, picture_url, int(rid)))
                except:
                    logger.error(traceback.format_exc())
                    return log_err(config[message_by_language]['INSERT_USER_STATUS'])
                    
                if is_email_exist:
                    try:
                        # Constructing the query to insert email of an user into user_emails table
                        insertQuery = "INSERT INTO `user_emails` (`rid`, `user_id`, `email`) VALUES (%s, %s ,AES_ENCRYPT(%s, %s))"
                        # Executing the query using
                        cursor.execute(insertQuery, (rid, user_id, email, key))
                    except:
                        logger.error(traceback.format_exc())
                        return log_err (config[message_by_language]['INSERT_USER_EMAIL_STATUS'])
                
                # Commented the below code due to the remove of Facebook Friends Functionality        
                # calling facebooksignup asynchronous partition
                #try:
                #    if is_already_exist:
                #       payload = {'profile':profile,'rid':old_rid}
                #        invokeLam.invoke(FunctionName="ProfilesFacebookSignInAsync" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
                #    else:
                #        payload = {'profile':profile,'rid':rid, 'user_id':user_id, "referral_code":referral_code}
                #        invokeLam.invoke(FunctionName="ProfilesFacebookSignUpAsync" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
                #except:
                #    logger.error(traceback.format_exc())
                #    return log_err (config[message_by_language]['INVOKING_ASYNC_STATUS'])
                
                try:
                    if is_already_exist==False and referral_code != "null":
                        payload = {'profile':profile,'rid':rid, 'user_id':user_id, "referral_code":referral_code}
                        invokeLam.invoke(FunctionName="ProfilesFacebookSignUpAsync" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
                except:
                    logger.error(traceback.format_exc())
                    return log_err (config[message_by_language]['INVOKING_ASYNC_STATUS'])
                    
                try:
                    # inserting user permission of a user
                    updationQuery = "UPDATE `users` SET `is_active`=1 WHERE `primary_email` IS NOT NULL AND `id` IN (SELECT `rid` FROM `user_permissions` WHERE `is_customized`=1 AND `rid`=%s)"
                    # Executing the query using cursor
                    cursor.execute(updationQuery, (int(rid)))
                    
                    # query for getting is_active flag related to the user to check that user has completed the test or not
                    selectionQuery = "SELECT `is_active` FROM `users` WHERE `id`=%s"
                    # Executing the query using cursor
                    cursor.execute(selectionQuery, (int(rid)))
                    
                    user_data = []
                    # getting the result from the cursor
                    for result in cursor: user_data.append(result)
                    
                    # getting the is_active field related to user from result list
                    is_active = int(user_data[0][0])
                except:
                    # If there is any error in above operations, logging the error
                    logger.error(traceback.format_exc())
                    return log_err(config[message_by_language]['IS_ACTIVE_STATUS'])
                    
                    
                if is_active == 1:
                    try:
                        # preparing payload for the lambda call
                        payload = {'rid' : rid}
                        
                        # calling the lambda function asynchronously to scrape the users profile
                        invokeLam.invoke(FunctionName="ProfilesFacebookScrapeImage" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
                    except:
                        # when there is some problem in above code
                        logger.error(traceback.format_exc())
                        return log_err (config[message_by_language]['INVOKING_ASYNC_STATUS'])
                
                # creating a payload for generating authentication token
                payload = {}
                if is_already_exist:
                    payload['id'] = old_rid
                    payload['user_id'] = old_userid
                else:
                    payload['id'] = rid
                    payload['user_id'] = user_id
                
                payload['name'] = name
                payload['language_id'] = language_id
                payload['exp'] = datetime.timestamp(datetime.now() + timedelta(days=int(TOKEN_EXPIRY_TIME)))
                try:
                    # generating an authentication token of a user
                    token =  jwt.encode(payload, SECRET_KEY)
                    logger.info("token: " + str(token)[2:-1])
                except:
                    return log_err (config[message_by_language]['TOKEN_STATUS'])
                    
                if is_already_exist:
                    # Returning a json response to the request by using required data 
                    return  {
                            'statusCode': 200,
                            'headers':{
                                        'Access-Control-Allow-Origin': '*',
                                        'Access-Control-Allow-Credentials': 'true'
                                      },
                            'body': json.dumps({'auth':token.decode('utf-8'), 'old_user_id':old_userid,'user_id':user_id, 'old_rid':old_rid, 'rid':rid, 'is_account_already_exist':is_already_exist, "firstname":firstname,"language_id":language_id})
                            }
                else:
                    # Returning a json response to the request by using required data 
                    return  {
                            'statusCode': 200,
                            'headers':{
                                        'Access-Control-Allow-Origin': '*',
                                        'Access-Control-Allow-Credentials': 'true'
                                      },
                            'body': json.dumps({'auth':token.decode('utf-8'), 'user_id':user_id, 'rid':rid, 'is_account_already_exist':is_already_exist, "firstname":firstname,"language_id":language_id})
                            }
        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['CONNECTION_STATUS'])
    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['FACEBOOK_DATA_STATUS'])

if __name__== "__main__":
    handler(None,None)