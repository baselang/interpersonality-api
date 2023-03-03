"""API Module to get all the notifications in order of date.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching data required for api
- getting all the notifications of user from the database
- sending the success json with the required data i.e. all notifications

"""

import jwt
import json
import pymysql
import logging
import traceback
from os import environ
import configparser
from datetime import datetime

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('getnotifications.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# Getting key for getting token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# getting message variable

message_by_language = "165_MESSAGES"
PROFILE_IMAGE_NOTIFICATION_TYPE = 2

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

def handler(event,context):
    """Function to handle the request for notifications API"""
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
        # getting data from the users request
        auth_token = event['headers']['Authorization']
        timezone_offset = int(event['headers']['timezone_offset'])
        if timezone_offset < 0:
            timezone_offset = "DATE_ADD(`timestamp`, INTERVAL " + str(abs(timezone_offset)) + " MINUTE)"
        elif timezone_offset > 0:
            timezone_offset = "DATE_SUB(`timestamp`, INTERVAL " + str(timezone_offset) + " MINUTE)"
        else:
            timezone_offset = "`timestamp`"
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
    
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
            # query to get notification details of the user
            query = "SELECT COUNT(*), `language_id` FROM `users` WHERE `id`=%s"
            # excecuting the query
            cursor.execute(query, (rid))
            users_list = []
            # getting results list from cursor
            for result in cursor: users_list.append(result)
            # checking any user with particular rid and user_id exist or not
            count = users_list[0][0]
            
            # getting current language_id of the user
            language_id = users_list[0][1]
            message_by_language = str(language_id) + "_MESSAGES"
            
            if count == 0:
                return log_err(config[message_by_language]['INVALID_USER'], 404)
        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
            
        try:
            try:
                # query to get notification details of the user
                selectionQuery = "SELECT `notification_type`, `json`, " + timezone_offset + " FROM `notifications` WHERE `rid`=%s ORDER BY `timestamp` DESC"
                # executing the query
                cursor.execute(selectionQuery, (rid))
                
                result_list = []
                
                # getting results list from cursor and preparing json list
                for result in cursor:
                    result_list.append({"notification_type": result[0], "notification_json": json.loads(result[1]),
                                        "timestamp": result[2].strftime("%B %d, %Y"),
                                        "datetime": result[2].strftime("%B %d, %Y %H:%M:%S")})

                    # Commented the below code due to the removal of Facebook Friends Functionality

                    # notification_type = result[0]
                    # if notification_type == PROFILE_IMAGE_NOTIFICATION_TYPE:
                    #     notification_json = json.loads(result[1])
                    #     profile_link = notification_json.get("profile_link")
                    #     user_id = profile_link.replace("/profile/", '')
                    #     # Getting the cursor from the DB connection to execute the queries
                    #     cursor1 = cnx.cursor()
                    #     selectionQuery = "SELECT `picture_url` FROM `users` WHERE `user_id`=%s"
                    #     # executing the query
                    #     cursor1.execute(selectionQuery, (user_id))
                    #     user_list = []
                    #     # fetching result from the cursor
                    #     for user_result in cursor1: user_list.append(user_result)
                    #     #getting current picture_url of the user
                    #     if user_list:
                    #         picture_url = user_list[0][0]
                    #     else:
                    #         picture_url = None
                    #     notification_json['profile_image'] = picture_url
                    #
                    #     result_list.append({"notification_type":result[0],"notification_json":notification_json,"timestamp":result[2].strftime("%B %d, %Y"),"datetime":result[2].strftime("%B %d, %Y %H:%M:%S")})
                    # else:
                    #     result_list.append({"notification_type":result[0],"notification_json":json.loads(result[1]),"timestamp":result[2].strftime("%B %d, %Y"),"datetime":result[2].strftime("%B %d, %Y %H:%M:%S")})
                
                logger.info(result_list)
                
                # getting unique dates
                unique_dates = list(set([i['timestamp'] for i in result_list]))
                
                # sorting the unique dates
                unique_dates.sort(key = lambda date: datetime.strptime(date, '%B %d, %Y'), reverse=True)
                
                ans = []
                
                # getting current date
                current_date = datetime.utcnow().strftime("%B %d, %Y")
                
                logger.info(current_date)
                
                # getting ans list
                for i in unique_dates:
                    month=i.split()[0].upper()
                    month = "MONTH_"+month
                    month_by_language=(config[message_by_language][month])
                    date_by_language=i.replace(i.split()[0],month_by_language)
                    if language_id == 245:
                        x= date_by_language.replace(",","")
                        x = x.split(" ")
                        date_by_language= (config[message_by_language]['DATE_FORMAT']).format(x[1],x[0],x[2])
                    if i == current_date:
                        ans.append({(config[message_by_language]['TODAY']):[{"notification_type":j["notification_type"],"notification_json":j["notification_json"],"datetime":date_by_language} for j in result_list if i==j["timestamp"]]})
                    else:
                        ans.append({date_by_language:[{"notification_type":j["notification_type"],"notification_json":j["notification_json"],"datetime":date_by_language} for j in result_list if i==j["timestamp"]]})
                
                # query to set visited status of all notifications when bell icon is clicked
                updationQuery = "UPDATE `notifications` SET `visited`=1 WHERE `rid`=%s AND `visited`=0"
                # executing the query
                # cursor.execute(updationQuery, (rid))
                
                # preparing success json  with result_list
                return {
                            'statusCode': 200,
                            'headers':{
                                        'Access-Control-Allow-Origin': '*',
                                        'Access-Control-Allow-Credentials': 'true'
                                      },
                            'body': json.dumps(ans)
                        }
            except:
                logger.error(traceback.format_exc())
                return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
           
        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)
        
if __name__== "__main__":
    handler(None,None)

