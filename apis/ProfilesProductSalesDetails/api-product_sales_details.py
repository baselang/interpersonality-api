"""API For getting detail of a Guide or Product sales page.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching data from request
- getting detail of a particular product
- Returning the JSON response with success status code and required data
"""

import jwt
import json
import pymysql
import logging
import traceback
from os import environ
import configparser

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('product_sales_details.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

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
    language_id = int(payload['language_id'])
    return rid, user_id, language_id

def handler(event,context):
    """Function to handle the request for upload picture API"""
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
        # If there is any error in above operations
        pass
    try:
        # Fetching data from event and rendering it
        auth_token = event['headers']['Authorization']
        product_id = int(event['headers']['product_id'])
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
    
    try:
        # verifying that the user is authorized or not to see this api's data
        rid, user_id, language_id = jwt_verify(auth_token)
        message_by_language = str(language_id) + "_MESSAGES"
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
            
            if language_id == 165:
                # Query for getting details of a product which is available to an user
                selectionQuery = "SELECT p.`id`, p.`amount`, (SELECT `currency_symbol` FROM `supported_currency` WHERE `id` = p.`currency_code`) AS `currency_symbol`, (SELECT `currency_code` FROM `supported_currency` WHERE `id` = p.`currency_code`) AS `currency_code` FROM `products` p WHERE p.`id`=%s"
                # Executing the Query
                cursor.execute(selectionQuery, (product_id))
            else:
                # Query for getting details of a product which is available to an user
                selectionQuery = "SELECT pt.`product_id`, pt.`amount`, (SELECT `currency_symbol` FROM `supported_currency` WHERE `id` = pt.`currency_code`) AS `currency_symbol`, (SELECT `currency_code` FROM `supported_currency` WHERE `id` = pt.`currency_code`) AS `currency_code` FROM `products_translations` pt WHERE pt.`id`=%s AND pt.`language_id`=%s"
                # Executing the Query
                cursor.execute(selectionQuery, (product_id, language_id))
                
            product_details = []
            # getting result from cursor
            for result in cursor: product_details.append({"product_id":result[0],"amount":result[1], "currency_symbol":result[2], "currency_code":result[3]})
            
            # returning success json
            return {
                        'statusCode': 200,
                        'headers':{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                            },
                        'body': json.dumps(product_details[0])
                    }
        except:
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
    
if __name__== "__main__":
    handler(None,None)