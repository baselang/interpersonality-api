"""API Module to be called asynchronously to send notification to users friends
It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- getting the required data to complete the process
- sending notifications to the users friends to tell about joining of the user
- Returning the JSON response with success status code

"""

import json
import pymysql
import logging
import traceback
from os import environ
import configparser
from datetime import datetime

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('send_friends_notifications_async.properties', encoding = "ISO-8859-1")


# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')
PROFILES_LINK = environ.get('PROFILES_LINK')
MYSTERY_UNLOCK_USER_COUNT = environ.get('MYSTERY_UNLOCK_USER_COUNT')
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

def log_err(errmsg, status_code):
    """Function to log the error messages."""
    return  {
                "statusCode": status_code,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def handler(event, context):
    logger.info(event)
    global message_by_language
    global MYSTERY_UNLOCK_USER_COUNT
    try:
        # fetching user data from event json
        rid = event['rid']
        try:
            friends_status = event['friends_status']
        except:
            friends_status = 'has_friends'
        MYSTERY_UNLOCK_USER_COUNT = int(MYSTERY_UNLOCK_USER_COUNT)
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
        
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        
        try:
            # getting the is_active field from the users table to check that the test is complete or not
            selectionQuery = "SELECT `is_active`,`picture_url`,CAST(AES_DECRYPT(`name`, %s) AS CHAR),`user_id`,`referral_code`, `gender` FROM `users` WHERE `id`=%s"
            # Executing the query
            cursor.execute(selectionQuery, (key, rid))
            user_data = []
            # getting the result list from the cursor
            for result in cursor: user_data.append(result)
            # getting the value of is_active field from the list
            is_active = int(user_data[0][0])
            picture_url = user_data[0][1]
            name = user_data[0][2]
            user_id = user_data[0][3]
            referral_code = user_data[0][4]
            gender = int(user_data[0][5])
            logger.info(user_data)
        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['IS_ACTIVE_STATUS'], 500)
            
        if referral_code != None and is_active == 1:
            
            # if user is reffered by any user than executing the below code
            try:
                selection_list = []
                # Query for getting details of user who has referred current new user
                selectionQuery = "SELECT `id`,`mystery_status`,`user_id`,`social_userid` FROM `users` WHERE `user_id`=%s"
                # Executing the query
                cursor.execute(selectionQuery,(referral_code))
                
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
                    cursor.execute(updateQuery, (MYSTERY_UNLOCK_USER_COUNT-1, MYSTERY_UNLOCK_USER_COUNT, referral_rid))
                    
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
                        notification_type = (MYSTERY_UNLOCK_USER_COUNT - mystery_friend_join_counter) - 1
                        
                        # giving notification for joining of the user
                        insertQuery = "INSERT INTO `notifications` (`rid`, `user_id`, `json`, `notification_type`) VALUES (%s, %s, %s, %s)"
                        # Executing the query
                        cursor.execute(insertQuery, (referral_rid, referral_user_id, json.dumps({"profile_link":PROFILES_LINK + referral_user_id}), notification_type_list[notification_type]))
                elif mystery_status == 0:
                    # updating the friends counter whose mystery is active and also mystery status
                    updateQuery = "UPDATE `users` SET `mystery_status`= CASE WHEN `mystery_friend_join_counter` >= %s AND `mystery_status`=0 THEN 2 ELSE `mystery_status` END, `mystery_friend_join_counter`=`mystery_friend_join_counter`+1 WHERE `id` = %s AND `mystery_status` IN (0,1)"
                    logger.info(updateQuery)
                    # Executing the query
                    cursor.execute(updateQuery, (MYSTERY_UNLOCK_USER_COUNT-1, referral_rid))
            except:
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['UPDATE_MYSTERY_COUNTER_STATUS'])
            
        if is_active == 1:

            # Commented the below code due to the removal of Facebook Friends Functionality

            # try:
            #     # Query for getting friends details of a user
            #     query = "SELECT `id`,`social_userid`,cast(AES_DECRYPT(`name`,%s) as char), `user_id` FROM `users` WHERE `id` NOT IN (SELECT `rid` FROM `notifications` WHERE `timestamp` BETWEEN \"" + datetime.utcnow().strftime("%Y-%m-%d") + " 00:00:00\" AND \"" + datetime.utcnow().strftime("%Y-%m-%d") + " 23:59:59\" AND `notification_type` = 2 AND `rid` IN (SELECT `id` FROM `users` WHERE `social_userid` IN (SELECT `friend_id` FROM `user_friends` WHERE `rid`=%s)) GROUP BY `rid` having COUNT(`rid`)>=3) and `social_userid` IN (SELECT `friend_id` FROM `user_friends` WHERE `rid`=%s)"
            #     # Executing the query
            #     cursor.execute(query, (key, rid, rid))
            #     result_list = []
            #     # fetching result from the cursor
            #     for result in cursor: result_list.append(result)
            #     logger.info(result_list)
            # except:
            #     logger.error(traceback.format_exc())
            #     return log_err(config[message_by_language]['FRIENDS_INFO'], 500)
            #
            # if result_list != []:
            #     # preparing input for the below query name, rid, user_id, picture
            #     friends_to_notify = tuple([tuple([int(i[0]), i[3], json.dumps({ "profile_image" : picture_url, "profile_name" : name, "profile_link": PROFILES_LINK + user_id, 'gender': gender})])  for i in result_list])
            #
            #     try:
            #         # Query for giving notifications to all friends about joining of current user
            #         query = "INSERT INTO `notifications` (`rid`, `user_id`, `json`, `notification_type`) VALUES (%s, %s, %s, 2)"
            #         # Executing the query
            #         cursor.executemany(query, friends_to_notify)
            #     except:
            #         logger.error(traceback.format_exc())
            #         return log_err(config[message_by_language]['NOTIFICATIONS_INFO'], 500)
                    
            # If none of the above conditions occur, return the default response
            return {
                        'statusCode': 200,
                        'headers':  {
                                    'Access-Control-Allow-Origin': '*',
                                    'Access-Control-Allow-Credentials': 'true'
                                    },
                        'body': json.dumps({'responses_saved':config[message_by_language]['SUCCESS_MESSAGE']})
                    }
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)
    finally:
        try:
            # Finally, clean up the connection
            cnx.close()
        except:
            pass