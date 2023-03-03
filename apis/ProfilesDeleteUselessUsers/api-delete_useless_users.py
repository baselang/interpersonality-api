"""
Lambda Function to delete all the users account who have not accepted terms and condition from the last week 
after giving the test and also not accepted the mandatory terms and condition

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- creating a connection
- deleting all the users that have not accepted terms and condition and not accepted mandatory condition
- Returning the JSON response with success status code

"""
import json
import logging
import traceback
import configparser
import pymysql
from os import environ

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('delete_useless_users.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

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
    return  {
                "statusCode": status_code,
                "body": json.dumps({"message":errmsg}),
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def handler(event,context):
    """Function to handle the request for Delete Useless Users API"""
    global message_by_language
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        
        try:
            # getting all the users that have not accepted terms and condition and not accepted mandatory condition
            selectionQuery = "SELECT `id`, `social_userid` FROM `users` WHERE DATE_ADD(`timestamp`, INTERVAL 7 DAY) < NOW() AND `is_active`=0"
            # excecuting the query
            cursor.execute(selectionQuery)
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            logger.info(result_list)
            
            if result_list == []:
                return {
                           'status_code':200,
                           'body':{"message":config[message_by_language]['SUCCESS_MESSAGE']}
                        }
            
            # making the arguments for the next queries which contain list of all the users rids and social_userids
            rid_list = ",".join([str(i[0]) for i in result_list])

            # Commented the below code due to the removal of Facebook Friends Functionality
            # social_userid_list = ",".join([ "\"" + str(i[1]) + "\"" for i in result_list if i[1] != None])

        except:
            # if there is any error in above code
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
        
        try:
            # deleting all the users that have not accepted terms and condition and not accepted mandatory condition
            deletionQuery = "DELETE FROM `users` WHERE `id` IN (" + rid_list + ")"
            # executing the query
            cursor.execute(deletionQuery)

            # Commented the below code due to the removal of Facebook Friends Functionality
            # if social_userid_list:
            #     # deleting all the entries of users in their friends list
            #     deletionQuery = "DELETE FROM `user_friends` WHERE `friend_id` IN (" + social_userid_list + ")"
            #     # excecuting the query
            #     cursor.execute(deletionQuery)


        except:
            # if there is any error in above code
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
            
        # returning the success json after deleting all the useless users account and data
        logger.info("All useless data and account from the database")   
        return {
                    'status_code':200,
                    'body':{"message":config[message_by_language]['SUCCESS_MESSAGE']}
                }
    except:
        # if there is any error in above code
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
    finally:
        # Finally, clean up the connection
        cursor.close()
        cnx.close()
        
if __name__== "__main__":
    handler(None,None)
    