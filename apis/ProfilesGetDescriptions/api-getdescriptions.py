"""API Module to provide Fetching Descriptive Questions Functionalities.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- Fetching the active descriptive questions 
- Returning the JSON response with list of descriptive questions and success status code

"""

import json
import pymysql
import logging
import traceback
import random
from os import environ
import configparser

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('getdescriptions.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

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
    logger.error(errmsg)
    return  {
                "statusCode": 500,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def handler(event,context):
    """Function to handle the request for Get Description API."""
    global message_by_language
           
    try:
        # fetching language_id of a user from event headers
        language_id = event['headers']['language_id']
        message_by_language = str(language_id) + "_MESSAGES"
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'])
    
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        
        try:
            if int(language_id)==165:
                # Constructing query to fetch descriptive questions (title & text) which are active
                query = "SELECT `id`, `description_title`, `description_text` FROM `descriptions` WHERE active = 1 and `language_id`=%s"
                # Executing the query using cursor
                cursor.execute(query, (language_id))
            else:
                # Constructing query to fetch descriptive questions (title & text) which are active
                query = "SELECT `description_id`, `description_title`, `description_text` FROM `description_translations` WHERE active = 1 and `language_id`=%s"
                # Executing the query using cursor
                cursor.execute(query, (language_id))
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'])

        
        results_list = []
        # Iterating through all results and preparing a list
        for result in cursor: results_list.append({"id":result[0],"description_title":result[1],'description_text':result[2]})
        random.shuffle(results_list)
        
        # Returning JSON response           
        return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                } ,
                'body': json.dumps(results_list[0:15])
            }
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'])

    finally:
        try:
            # Finally, clean up the connection
            cnx.close()
            cursor.close()
        except: 
            pass 

if __name__== "__main__":
    handler(None,None)
