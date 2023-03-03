"""
Lambda Function for checking that an user accepted the mandatory terms or condition or not

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- creating a connection
- getting the status that user has accepted the mandatory terms or condition or not 
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
config.read('termsandconditioncheck.properties', encoding = "ISO-8859-1")

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
    """Function to handle the request for Profiles Terms and Condition Checking API"""
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
        
    test_status = "not_completed"
    logger.info(event)
    try:
        # getting required data from event
        rid = event['headers']['rid']
        language_id = event['headers']['language_id']
        message_by_language = str(language_id) + "_MESSAGES"
        if rid == "null" or rid == None:
            # if there is any error in above code
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
    except:
        # if there is any error in above code
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
        
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        
        try:
            # getting response that these user has accepted the terms and condition or not
            selectionQuery = "SELECT CASE WHEN COUNT(*) > 0 AND `is_customized`=1 THEN \"accepted\" ELSE \"not_accepted\" END FROM `user_permissions` WHERE `rid`=%s"
            # excecuting the query
            cursor.execute(selectionQuery,(int(rid)))
            
            result_list = []
            # fetching data we got after execution of above query
            for result in cursor: result_list.append(result)
            
            # getting the test_status from the result_list
            terms_and_conditon = result_list[0][0]
            logger.info(terms_and_conditon)
        except:
            # if there is any error in above code
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
            
        # returning the success json after checking that user has accepted the mandatory terms or condition or not
        return {
                    'statusCode': 200,
                    'headers':{
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Credentials': 'true'
                        },
                    'body': json.dumps({"terms_and_conditon":terms_and_conditon})
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
    