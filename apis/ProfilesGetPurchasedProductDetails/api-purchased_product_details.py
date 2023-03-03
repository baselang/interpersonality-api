"""API For getting detail of purchased products by the user.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching data from request
- getting detail of all purchased products by the user
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
config.read('purchased_product_details.properties', encoding = "ISO-8859-1")

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
    rid = payload['id']
    user_id = payload['user_id']
    language_id = payload['language_id']
    return rid, user_id, language_id

def handler(event,context):
    """Function to handle the request for upload picture API"""
    global message_by_language
    
    logger.info(event)
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
    except:
        # if user does not have valid authorization
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['UNAUTHORIZED'], 403)
        
    message_by_language = str(language_id) + "_MESSAGES"
    
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
        
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
        
        # Query for getting product details of all the purchased product of a particular user
        selectionQuery = "SELECT `id`, `product_name`, `product_title`, `full_description` FROM `products` WHERE `status` = 1 AND `id`=%s"
        # Executing the Query
        cursor.execute(selectionQuery, (product_id))
        
        purchased_products = []
        # getting result from cursor
        for result in cursor: purchased_products.append({"product_id":result[0],"product_name":result[1],"product_title":result[2],"product_full_description":result[3]})
        
        # returning success json
        return {
                    'statusCode': 200,
                    'headers':{
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Credentials': 'true'
                        },
                        'body': json.dumps(purchased_products[0])
                }
        #'body': json.dumps({"purchased_products" : purchased_products[0]})
        #}
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['QUERY_STATUS'], 500)
        
if __name__== "__main__":
    handler(None,None)