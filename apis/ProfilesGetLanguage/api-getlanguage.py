#!/usr/bin/env python3

"""API Module to provide Fetching Location Functionalities.

It provides the following functionalities:
1. log_err(): Logging error and returning the JSON response with error message & status code
2. make_connection(): Connecting to the Database using connection details received through environment variables
3. handler(): Handling the incoming request with following steps:
- Fetching the user's country based on his source IP
- Checking the user's country against the allowed country list
- Getting language_id according to country code
- Returning JSON response including with success status code if country is in allowed list  else redirection to Coming Soon page

"""

import json
import logging
import pymysql
import traceback
import configparser
from os import environ
from accept_language import accept_language

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('getlanguage.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')
SUPPORTED_LANGUAGES = environ.get('SUPPORTED_LANGUAGES')
DEFAULT_LANGUAGE = int(environ.get('DEFAULT_LANGUAGE'))

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
# Setting the log level to DEBUG
logger.setLevel(logging_Level)

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

def handler(event, context):
    """Function to handle the request to Get Location API."""
    # Preparing the list of Latin American countries based on their ISO codes
    logger.info(event)
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
        # If there is any error in above operations, logging the error 
        pass
        
    try:
        # Getting the IP of the user from the request
        language_header = event['headers']['Accept-Language']
        parsed_lang_header = accept_language.parse_accept_language(language_header)
        client_language = getattr(parsed_lang_header[0],'language')
    except:
        client_language = None
    
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        try:
            languages = [int(i) for i in SUPPORTED_LANGUAGES.split(",")]
            if client_language != None:
                # Constructing the query to get language_id according to country code
                selectionQuery = "SELECT `id` FROM `language` WHERE `code`=%s"
                cursor.execute(selectionQuery, (client_language))
                try:
                    result_list = []
                    for result in cursor: result_list.append(result)
                    language_id = result_list[0][0]
                    if language_id not in languages or language_id == None:
                        language_id = DEFAULT_LANGUAGE
                except:
                    log_err (config['MESSAGES']['LANGUAGE_STATUS'])
                    language_id = DEFAULT_LANGUAGE
            else:
                language_id = DEFAULT_LANGUAGE
        except:
             log_err (config['MESSAGES']['QUERY_EXECUTION_STATUS'])
             language_id = DEFAULT_LANGUAGE
        
    except:
        log_err (config['MESSAGES']['CONNECTION_STATUS'])
        language_id = DEFAULT_LANGUAGE
    finally:
        # Finally, clean up the connection
        cursor.close()
        cnx.close()
    
    # returning JSON response with result
    return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
                'body': json.dumps({"language_id":language_id})
            }
            

if __name__== "__main__":
    handler(None,None)