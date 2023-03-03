"""API Module to provide Mystery details for user to show on mystery button.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. success_json(): Function to create json response for success message
3. log_err(): Logging error and returning the JSON response with error message & status code
4. jwt_verify(): verifying token and fetching data from the jwt token sent by user
5. handler(): Handling the incoming request with following steps:
- Fetching the data
- fetching users mystery information
- Returning the JSON response with the required data and success status code

"""


import jwt
import json
import pymysql
import logging
import traceback
from os import environ
from datetime import datetime
from datetime import timezone
from datetime import timedelta
import configparser
from math import floor

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('mystery_button.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')
PROFILES_LINK = environ.get('PROFILES_LINK')
mystery_unlock_user_count = environ.get('MYSTERY_UNLOCK_USER_COUNT')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

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

def success_json(success_data):
    """Function to create json response for success message"""
    return  {
                'statusCode': 200,
                'headers':{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                          },
                'body': json.dumps(success_data)
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

def handler(event,context):
    """Function to handle the request for mysterybutton api."""
    logger.info(event)
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
        auth_token = event['headers']['Authorization']
        mystery_unlock_user_count = int(mystery_unlock_user_count)
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
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        
        try:
            # Query for getting current language of the user
            selectionQuery = "SELECT `language_id` FROM `users` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (rid))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            # getting current language_id of the user 
            language_id = result_list[0][0]
            message_by_language = str(language_id) + "_MESSAGES"
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
        
        try:
            # selection query to update mystery start time and mystery status
            updationQuery = "UPDATE `users` SET `mystery_status`= CASE WHEN `mystery_status`=1 AND NOW()>DATE_ADD(`mystery_start_time`, INTERVAL 24 HOUR) AND `mystery_friend_join_counter` < %s THEN 3 ELSE `mystery_status` END WHERE `id`=%s"
            # Executing the updation query
            cursor.execute(updationQuery, (mystery_unlock_user_count, rid))
        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['UPDATION_STATUS'], 500)
        
        try:
            # selection query to get mystery data from user
            selectionQuery = "SELECT `id`, `mystery_status`, TIMEDIFF(DATE_ADD(`mystery_start_time`, INTERVAL 24 HOUR),NOW()), `mystery_friend_join_counter`, `is_mystery_visited` FROM `users` WHERE `id`=%s"
            cursor.execute(selectionQuery, (rid))
            result_list = []
            for result in cursor: result_list.append(result)
            rid = result_list[0][0]
            mystery_status = result_list[0][1]
            mystery_start_time = result_list[0][2]
            mystery_friend_join_counter = result_list[0][3]
            is_mystery_visited = result_list[0][4]
            logger.info("mystery_start_time :")
            logger.info(mystery_start_time)
            logger.info("####################")
            
            if mystery_status==1 :
                if mystery_start_time.days==0:
                    remaining_hrs = floor(mystery_start_time.seconds/3600)
                    if remaining_hrs<=4:
                        remaining_time = (config[message_by_language]['REMAINING_FEW_MINUTES'], config[message_by_language]['REMAINING_ONE_HOUR'], config[message_by_language]['REMAINING_TWO_HOURS'], config[message_by_language]['REMAINING_THREE_HOURS'], config[message_by_language]['REMAINING_FOUR_HOURS'])
                        # Query for checking notification of 4 hours before reminder is present or not
                        selectionQuery = "SELECT COUNT(*) FROM `notifications` WHERE `rid`=%s AND `notification_type`=6"
                        # Executing the Query
                        cursor.execute(selectionQuery, (rid))
                        
                        notification_count = []
                        # getting result from cursor to get notification_count
                        for result in cursor: notification_count.append(result)
                        
                        if notification_count[0][0] == 0:
                            # giving notification for mystery unlock
                            insertQuery = "INSERT INTO `notifications` (`rid`, `user_id`, `json`, `notification_type`) VALUES (%s, %s, %s, %s)"
                            # Executing the query
                            cursor.execute(insertQuery, (rid, user_id, json.dumps({"remaining_friends":mystery_unlock_user_count - mystery_friend_join_counter, "remaining_time" : remaining_time[remaining_hrs]}), 6))
                
            if mystery_status == 1:
                # when mystery timer is started
                return  success_json({'mystery_status':mystery_status, 'mystery_start_time':mystery_start_time.seconds})
                
            elif mystery_status == 2:
                # returning the success json with required data
                return  success_json({'mystery_status':mystery_status, 'is_mystery_visited':int(is_mystery_visited), 'message':config[message_by_language]['MYSTERY_STATUS_SUCCESSFULLY_UNLOCKED']})
                
            elif mystery_status == 3:
                # when user failed to unlock mystery
                return  success_json({'mystery_status':mystery_status, 'message':config[message_by_language]['MYSTERY_STATUS_UNSUCCESSFULLY_UNLOCKED']})
                
            else:
                # when mystery timer is not started
                return  success_json({'mystery_status':mystery_status, 'message':config[message_by_language]['MYSTERY_STATUS']})
                
        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
            
    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)
    finally:
        try:
            # Finally, clean up the connection
            cnx.close()
        except: 
            pass 
    

if __name__== "__main__":
    handler(None,None)