"""API For getting all the product or guides details.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching data from request
- getting all the products associated data respect to the products that need to be displayed
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
config.read('get_products.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

# api related variables
message_by_language = "165_MESSAGES"
DEFAULT_LANGUAGE = "165"
cancellation_time = environ.get('CANCELLATION_TIME')

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
    """Function to handle the request for getting all the guides information API"""
    global message_by_language
    global language_id
    
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
        # Fetching data from event and rendering it
        auth_token = event['headers']['Authorization']
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
        
        try:
            # Query for getting current language of the user
            selectionQuery = "SELECT `language_id`, (SELECT `is_privacy_consent` FROM `user_permissions` WHERE `rid` = %s) AS `is_privacy_consent` FROM `users` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (rid, rid))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            # getting current language_id of the user 
            language_id = int(result_list[0][0])
            is_privacy_consent = int(result_list[0][1])
            message_by_language = str(language_id) + "_MESSAGES"
            
            if language_id==int(DEFAULT_LANGUAGE):
                # Executing the following code when language_id is default
                
                # Query for getting all the guides details from the products
                selectionQuery = "SELECT p.`id`,p.`amount`, (SELECT `currency_symbol` FROM `supported_currency` WHERE `id` = p.`currency_code`) AS `currency_symbol`, (SELECT `currency_code` FROM `supported_currency` WHERE `id` = p.`currency_code`) AS `currency_code` FROM `products` p WHERE p.`id` NOT IN (3) AND `status` = 1"
                # Executing the Query
                cursor.execute(selectionQuery, None)
                
            else:
                # Executing the query when language is not default language
                
                # Query for getting all the guides details from the products_translation
                selectionQuery = "SELECT p.`id`,p.`amount`, (SELECT `currency_symbol` FROM `supported_currency` WHERE `id` = p.`currency_code`) AS `currency_symbol`, (SELECT `currency_code` FROM `supported_currency` WHERE `id` = p.`currency_code`) AS `currency_code` FROM `products_translations` p WHERE p.`language_id` = %s AND p.`product_id` NOT IN (3) AND `status` = 1"
                # Executing the Query
                cursor.execute(selectionQuery, language_id)
                
            products_list = {}
            
            # getting result from cursor
            for result in cursor: 
                products_list[result[0]] = {"amount":result[1], "currency_symbol":result[2], "currency_code":result[3], "user_purchases":[]}
                
            logger.info(products_list)
            
            # Query for getting purchased and guides details where the output is accordingly partner is selected or not selected
            selectionQuery = "(SELECT (SELECT `transaction_date` FROM `user_product` WHERE `id` = `user_product_id`) as `timestamp`, up.`product_id`, NULL as `user_product_id`, NULL as `is_cancelled`, up.`id` as `user_partner_id`, (SELECT CAST(AES_DECRYPT(`name`, %s) AS CHAR) FROM `users` WHERE `id`= up.`partner_rid`) AS `name`, (SELECT `picture_url` FROM `users` WHERE `id`= up.`partner_rid`) AS `picture_url`, (SELECT `is_privacy_consent` FROM `user_permissions` WHERE `rid`= up.`partner_rid`) AS `is_privacy_consent` FROM `user_partner_products` up WHERE up.`user_rid`=%s ORDER BY `purchase_timestamp` DESC) UNION ALL (SELECT `transaction_date`, `product_id`, `id`, (CASE WHEN (NOW() > DATE_ADD(`transaction_date`, INTERVAL %s MINUTE)) OR (`id`=(SELECT upp.`id` FROM `user_product` upp WHERE  upp.`rid`=%s ORDER BY upp.`transaction_date` LIMIT 1)) THEN 0 ELSE 1 END), NULL, NULL, NULL, NULL  FROM `user_product` WHERE `rid`=%s AND `id` NOT IN (SELECT DISTINCT(`user_product_id`) FROM `user_partner_products`) ORDER BY `transaction_date` DESC) ORDER BY `timestamp` DESC"
            # Executing the Query
            cursor.execute(selectionQuery, (key, int(rid), int(cancellation_time), int(rid), int(rid)))
            
            # getting result from cursor
            for result in cursor:
                (products_list[result[1]]['user_purchases']).append({"user_product_id" : result[2], "is_cancelled" : result[3],"user_partner_id" : result[4], "name" : result[5], "picture_url" : result[6], "partner_privacy_consent":result[7]})
                
            # returning success json
            return {
                        'statusCode': 200,
                        'headers':{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                            },
                        'body': json.dumps({"user_privacy_consent":is_privacy_consent, "products_list":products_list})
                    }
        except:
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
    finally:
        cnx.close()
        cursor.close()
        
if __name__== "__main__":
    handler(None,None)