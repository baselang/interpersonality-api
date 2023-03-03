"""API Module to make a user signup after completing test.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- Getting data from users i.e. his email, firstname, lastname, password
- saving users cridentials into our database to create his profile
- Returning the JSON response with message and success status code

"""
import jwt
import json
import pymysql
import logging
import traceback
from os import environ
from datetime import datetime  
from datetime import timedelta
import configparser
import boto3

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('signup.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')
PROFILES_LINK = environ.get('PROFILES_LINK')
mystery_unlock_user_count = environ.get('MYSTERY_UNLOCK_USER_COUNT')

# Getting key for getting token
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')
key = environ.get('DB_ENCRYPTION_KEY')

# Variables related to s3 bucket
AWS_REGION =  environ.get('REGION')
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

def handler(event,context):
    """Function to handle the request for Get Big5 API."""
    global message_by_language
    global mystery_unlock_user_count
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
        # Fetching data from event and rendering it
        language_id = event['headers']['language_id']
        message_by_language = str(language_id) + "_MESSAGES"
        data = json.loads(event['body'])
        rid = data['rid']
        email = data['email'].lower()
        firstname = data['firstname']
        lastname = data['lastname']
        password = data['password']
        name = data['firstname'] + " " + data['lastname']
        mystery_unlock_user_count = int(mystery_unlock_user_count)
        
        try:
            referral_code = event['headers']['referral_code']
            if referral_code != "null":
                referral_code = "\"" + referral_code + "\""
        except:
            referral_code = "null"
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        try:
            # checking that a user with same email id exist or not
            query = "SELECT `user_id` FROM `user_emails` WHERE `email`=AES_ENCRYPT(%s,%s)"
            cursor.execute(query, (email, key))
            user_exist = []
            for result in cursor: user_exist.append(result)
            
            if len(user_exist) > 0:
                return log_err (config[message_by_language]['EMAIL_ID_STATUS'], 400)
            
            # Constructing the query to get user_id of a user
            selectionQuery = "SELECT `user_id` FROM `users` WHERE `id`=%s"
            cursor.execute(selectionQuery, (rid))
            try:
                result_list = []
                for result in cursor: result_list.append(result)
                try:
                    user_id = result_list[0][0]
                except:
                    logger.error(traceback.format_exc())
                    return log_err (config[message_by_language]['USER_STATUS'], 500)
                try:
                    # Constructing the query to insert email of an user into user_emails table
                    insertQuery = "INSERT INTO `user_emails` (`rid`, `user_id`, `email`) VALUES (%s, %s ,AES_ENCRYPT(%s, %s))"
                    # Executing the query using
                    cursor.execute(insertQuery, (rid, user_id, email, key))
                except:
                    logger.error(traceback.format_exc())
                    return log_err (config[message_by_language]['EMAIL_STATUS'], 500)
                
                if referral_code != "null":
                    # checking that a referral user exist or not
                    selectionQuery = "SELECT `user_id` FROM `users` WHERE `user_id`=" + referral_code
                    cursor.execute(selectionQuery)
                    referral_user_exist = []
                    for ref_result in cursor: referral_user_exist.append(ref_result)
                    
                    if len(referral_user_exist) == 0:
                        referral_code = "null"
                    
                # Constructing the query to fill user cridentials into users table
                updateQuery = "UPDATE `users` SET `firstname`=AES_ENCRYPT(%s, %s), `lastname`=AES_ENCRYPT(%s, %s), `name`=AES_ENCRYPT(%s, %s),   `password`=AES_ENCRYPT(%s, %s), `primary_email`=AES_ENCRYPT(%s, %s), `referral_code`=" + referral_code + " WHERE `id`=%s"
                # Executing the query using
                cursor.execute(updateQuery, (firstname, key, lastname, key, name, key, password, key, email, key, rid))
                
                if referral_code != "null":
                    # if user is reffered by any user than executing the below code
                    try:
                        
                        selection_list = []
                        # Query for getting details of user who has referred current new user
                        selectionQuery = "SELECT `id`,`mystery_status`,`user_id`,`social_userid` FROM `users` WHERE `user_id`=" + referral_code
                        # Executing the query
                        cursor.execute(selectionQuery)
                         
                        # Fetching results from the above query
                        for result in cursor: selection_list.append(result)
                        # getting mystery_status, id, user_id and social_userid from the list from above query
                        mystery_status = selection_list[0][1]
                        referral_rid = selection_list[0][0]
                        referral_user_id = selection_list[0][2]
                        referral_social_user_id = selection_list[0][3]
                        
                        
                        if mystery_status == 1:
                            # updating the friends counter whose mystery is active and also mystery status
                            updateQuery = "UPDATE `users` SET `mystery_status`= CASE WHEN `mystery_friend_join_counter` >= %s AND `mystery_status`=1 AND NOW() < DATE_ADD(`mystery_start_time`, INTERVAL 24 HOUR) THEN 2 WHEN `mystery_friend_join_counter` < %s AND `mystery_status`=1 AND NOW() > DATE_ADD(`mystery_start_time`, INTERVAL 24 HOUR) THEN 3 ELSE `mystery_status` END, `mystery_friend_join_counter`=`mystery_friend_join_counter`+1 WHERE `id` = %s AND `mystery_status` IN (0,1)"
                            logger.info(updateQuery)
                            # Executing the query
                            cursor.execute(updateQuery, (mystery_unlock_user_count-1, mystery_unlock_user_count, referral_rid))
                            
                            selection_list = []
                            # selection query for getting mystery_status
                            selectionQuery = "SELECT `mystery_status`, `mystery_friend_join_counter` FROM `users` WHERE `id`=%s"
                            # Executing the query
                            cursor.execute(selectionQuery, (referral_rid))
                            
                            # getting result from the above query
                            for result in cursor: selection_list.append(result)
                            mystery_status = selection_list[0][0]
                            mystery_friend_join_counter = int(selection_list[0][1])
                            
                            if mystery_status == 2 :
                                # if mystery_status is updated to 2 from 1 then mystery is unlocked so giving notification for it
                                
                                # giving notification for mystery unlock
                                insertQuery = "INSERT INTO `notifications` (`rid`, `user_id`, `json`, `notification_type`) VALUES (%s, %s, %s, %s)"
                                # Executing the query
                                cursor.execute(insertQuery, (referral_rid, referral_user_id, "{}", 1))
                            elif mystery_status == 1 :
                                # notification type list for storing different types of notification according to the mystery_friend_join_counter
                                notification_type_list = [5,4]
                                
                                # the position of notification in the list
                                notification_type = (mystery_unlock_user_count - mystery_friend_join_counter) - 1
                                
                                # giving notification for joining of the user
                                insertQuery = "INSERT INTO `notifications` (`rid`, `user_id`, `json`, `notification_type`) VALUES (%s, %s, %s, %s)"
                                # Executing the query
                                cursor.execute(insertQuery, (referral_rid, referral_user_id, json.dumps({"profile_link":PROFILES_LINK + referral_user_id}), notification_type_list[notification_type]))
                        elif mystery_status == 0:
                            # updating the friends counter whose mystery is active and also mystery status
                            updateQuery = "UPDATE `users` SET `mystery_status`= CASE WHEN `mystery_friend_join_counter` >= %s AND `mystery_status`=0 THEN 2 ELSE `mystery_status` END, `mystery_friend_join_counter`=`mystery_friend_join_counter`+1 WHERE `id` = %s AND `mystery_status` IN (0,1)"
                            logger.info(updateQuery)
                            # Executing the query
                            cursor.execute(updateQuery, (mystery_unlock_user_count-1, referral_rid))
                    except:
                        logger.error(traceback.format_exc())
                        return log_err (config[message_by_language]['UPDATE_MYSTERY_COUNTER_STATUS'], 500)
                    
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
                    # calling the lambda function asynchronously to generate an generic image for similarity for sharing on facebook
                    payload = {'rid' : rid}
                    invokeLam.invoke(FunctionName="ProfilesGenericProfileImage" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
                except:
                    # when there is some problem in above code
                    logger.error(traceback.format_exc())
                    return log_err (config[message_by_language]['INVOKING_ASYNC_STATUS'], 500)
                    
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
                        
                cursor.close()
                
                payload = {}
                payload['id'] = rid
                payload['name'] = name
                payload['language_id'] = language_id
                payload['user_id'] = user_id
                payload['exp'] = datetime.timestamp(datetime.now() + timedelta(days=365))
                try:
                    token =  jwt.encode(payload, SECRET_KEY)
                    logger.info("token: " + str(token)[2:-1])
                except:
                    logger.error(traceback.format_exc())
                    return log_err (config[message_by_language]['TOKEN_STATUS'], 500)
                
                return {
                    'statusCode': 200,
                    'headers':{
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Credentials': 'true'
                        },
                    'body': json.dumps({'auth':token.decode('utf-8'), 'user_id':user_id, 'rid':rid, 'language_id':language_id})
                    }
            except:
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['INSERTION_STATUS'], 500)
        except:
            logger.error(traceback.format_exc())
            log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
    finally:
        try:
            # Finally, clean up the connection
            cnx.close()
        except: 
            pass
        
if __name__== "__main__":
    handler(None,None)