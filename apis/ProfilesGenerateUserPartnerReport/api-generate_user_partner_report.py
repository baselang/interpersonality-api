"""API For generating the report of the user and providing the id to fetch report content.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching data from request
- validating the request data
- inserting the report entry for the user and his partner
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
config.read('generate_user_partner_report.properties', encoding = "ISO-8859-1")

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
    """Function to handle the request for generate user partner report API"""
    global message_by_language
    logger.info(event)
    try:
        # Fetching data from event and rendering it
        auth_token = event['headers']['Authorization']
        purchase_id = int(event['headers']['purchase_id'])
        partner_user_id = event['headers']['partner_user_id']
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
            
            # Query for getting rid and product_id of the user
            selectionQuery = "SELECT `rid`,`product_id` FROM `user_product` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (int(purchase_id)))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            # getting current language_id of the user
            owner_rid = int(result_list[0][0])
            product_id = int(result_list[0][1])
            
            # Query for getting rid of the partner and checking that entry of the user for following partner id already exist or not
            selectionQuery = "SELECT CASE WHEN (SELECT COUNT(*) FROM `user_partner_products` WHERE `product_id`=%s AND `user_rid`=%s AND `partner_rid`=(SELECT `id` FROM `users` WHERE `user_id`=%s))<1 THEN (SELECT `id` FROM `users` WHERE `user_id`=%s) ELSE 0 END"
            # Executing the Query
            cursor.execute(selectionQuery, (int(product_id), int(rid), partner_user_id, partner_user_id))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            logger.info(result_list)
            
            # getting current language_id of the user
            partners_rid = int(result_list[0][0])
            
            # checking that if the user has already purchased same product with the requested partner or not
            if owner_rid == int(rid) and partners_rid != 0:
                
                if int(product_id) == 2 :
                    # preparing the data to insert rows
                    rows_to_insert = list([tuple([int(product_id), int(rid), int(partners_rid), int(purchase_id)]), tuple([int(product_id), int(partners_rid), int(rid), int(purchase_id)])])
                    # Query for getting details of a product which is available to an user
                    insertionQuery = "INSERT INTO `user_partner_products` (`product_id`, `user_rid`, `partner_rid`, `user_product_id`, `purchase_timestamp`) VALUES (%s, %s, %s, %s, NOW())"
                    # Executing the Query
                    cursor.executemany(insertionQuery, rows_to_insert)
                else:
                    # Query for getting details of a product which is available to an user
                    insertionQuery = "INSERT INTO `user_partner_products` (`product_id`, `user_rid`, `partner_rid`, `user_product_id`, `purchase_timestamp`) VALUES (%s, %s, %s, %s, NOW())"
                    # Executing the Query
                    cursor.execute(insertionQuery, (int(product_id), int(rid), int(partners_rid), int(purchase_id)))
            else:
                # if the user has already purchased the product with the given partner_rid and he is hitting this api than the user is making a bad request
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['INVALID_REQUEST'], 500)
            
            # Query for the user_partner_id for seeing the report of the user
            selectionQuery = "SELECT `id` FROM `user_partner_products` WHERE `user_product_id`=%s AND `user_rid`=%s AND `partner_rid`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (int(purchase_id), int(rid), int(partners_rid)))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            # Fetching the user_partner_id for the user to see the report
            user_partner_id = int(result_list[0][0])
            
            # returning success json with the required data
            return {
                        'statusCode': 200,
                        'headers':{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                            },
                        'body': json.dumps({"user_partner_id" : user_partner_id})
                    }
        except:
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)

if __name__== "__main__":
    handler(None,None)