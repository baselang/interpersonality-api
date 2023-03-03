"""API For making a payment of the product.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. make_client(): Function to make boto3 aws client
5. handler(): Handling the incoming request with following steps:
- Fetching data from request
- inserting product into user product purchase list
- Returning the JSON response with success status code and required data
"""

import boto3
import jwt
import json
import pymysql
import logging
import traceback
from os import environ
import configparser
import chargebee
import httpagentparser

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('makeproductpayment.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')
transaction_type = environ.get('TRANSACTION_TYPE')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

# Environment required for chargebee
SITE_KEY = environ.get('SITE_KEY')
SITE_URL = environ.get('SITE_URL')

# default variable for 8 dates product_id
#DEFAULT_8DATES = int(environ.get('DEFAULT_8DATES'))
DEFAULT_CHANNEL = int(environ.get('DEFAULT_CHANNEL'))

# aws cridentials required for creating boto3 client object
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')

# Required variables for api
is_cancelled = int(environ.get('IS_CANCELLED'))

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# getting message variable

message_by_language = "165_MESSAGES"
DEFAULT_LANGUAGE = "165"

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
        
def jwt_verify(auth_token):
    """Function to verify the authorization token"""
    # decoding the authorization token provided by user
    payload = jwt.decode(auth_token, SECRET_KEY, options={'require_exp': True})
    
    # setting the required values in return
    rid = int(payload['id'])
    user_id = payload['user_id']
    language_id = payload['language_id']
    return rid, user_id, language_id
    
def make_client():
    """Making a boto3 aws client to perform invoking of functions"""
    
    # creating an aws client object by providing different cridentials
    invokeLam = boto3.client(
                                "lambda", 
                                region_name=AWS_REGION,
                                aws_access_key_id=AWS_ACCESS_KEY,
                                aws_secret_access_key=AWS_SECRET
                            )
    # returning the object
    return invokeLam

def handler(event,context):
    """Function to handle the request for make product payment API"""
    global message_by_language
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
        user_agent = event['headers']['User-Agent']
        
        # # if the product id is for 8 dates product then
        # if int(product_id) == int(DEFAULT_8DATES) :
        #     user_partner_id = event['headers']['user_partner_id']
        
        # configuring chargebee object
        chargebee.configure(SITE_KEY,SITE_URL)
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
            selectionQuery = "SELECT `language_id`, `customer_id` FROM `users` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (rid))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            # getting current language_id of the user 
            language_id = result_list[0][0]
            customer_id = result_list[0][1]
            message_by_language = str(language_id) + "_MESSAGES"
            
            # Query for getting current language of the user
            selectionQuery = "SELECT COUNT(*) FROM `user_product` WHERE `rid`=%s and `status` in ('purchased')"
            # Executing the Query
            cursor.execute(selectionQuery, (rid))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            existence_counter = int(result_list[0][0])
            
            if existence_counter > 0 :
                # If user_product already exist
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['PRODUCT_EXISTENCE_STATUS'], 400)
                
            # Query for getting current language of the user
            selectionQuery = "SELECT `plan_id` FROM `products` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (product_id))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            plan_id = result_list[0][0]
            
            logger.info(result_list)
            
            # getting the subscriptions related to the customer from the chargebee
            entries = chargebee.Subscription.list({"customer_id[is]" : customer_id, "plan_id[is]" : plan_id, "sort_by[asc]" : "created_at"})
            
            logger.info(entries)
            
            if entries != []:
                for entry in entries:
                    # Fetching the subscription id of the first transaction
                    subscription_id = str(entry.subscription.id)
                    transaction_date = str(entry.subscription.created_at)
                    break
                    
                logger.info("transaction_date :::::::")
                logger.info(transaction_date)
                    
                # Query for inserting product into user purchased products list
                insertionQuery = "INSERT INTO `user_product` (`product_id`,`rid`,`transaction_date`,`status`,`subscription_id`) VALUES (%s, %s, NOW(), %s, %s)"
                # Executing the Query
                cursor.execute(insertionQuery, (product_id, rid, "purchased", subscription_id))
                
                # selecting the id of the transaction which got created
                selectionQuery = "SELECT `id`,`transaction_date` FROM `user_product` WHERE `product_id`=%s AND `rid`=%s AND `status`=%s AND `subscription_id`=%s"
                # Executing the Query
                cursor.execute(selectionQuery, (product_id, rid, "purchased", subscription_id))
                
                result_list = []
                
                # fetching result from the cursor
                for result in cursor: result_list.append(result)
                
                # Fetching the user product
                user_product_id = int(result_list[0][0])
                transaction_timestamp = result_list[0][1]
                
                logger.info("transaction_timestamp :::::::")
                logger.info(transaction_timestamp)
                
                if language_id==int(DEFAULT_LANGUAGE):
                    # Executing the following code when language_id is default
                    
                    # Query for getting all the guides details from the products
                    selectionQuery = "SELECT `currency_code`, `amount`  FROM `products` WHERE `id`=%s"
                    # Executing the Query
                    cursor.execute(selectionQuery, (int(product_id)))
                else:
                    # Executing the following code when language_id is default
                    
                    # Query for getting all the guides details from the products
                    selectionQuery = "SELECT `currency_code`, `amount`  FROM `products_translations` WHERE `product_id`=%s AND `language_id`=%s"
                    # Executing the Query
                    cursor.execute(selectionQuery, (int(product_id), int(language_id)))
                    
                result_list = []
                # fetching the result from the cursor
                for result in cursor : result_list.append(result)
                currency_code = int(result_list[0][0])
                amount = int(result_list[0][1])
                
                # parsing user_agent and then fetching os_name and browser_name
                parsed_json = httpagentparser.detect(user_agent)
                os_name = parsed_json['os']['name']
                browser_name = parsed_json['browser']['name']
                
                # inserting entry into users transaction table 
                insertionQuery = "INSERT INTO `user_transactions` (`product_id`, `transaction_type`, `currency_code`, `rid`, `user_id`, `transaction_timestamp`, `amount`, `os_name`, `browser_name`, `subscription_id`, `customer_id`, `acquisition_channel`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                # Executin the query
                cursor.execute(insertionQuery, (int(product_id), int(transaction_type), currency_code, int(rid), user_id, transaction_timestamp, amount, os_name, browser_name, subscription_id, customer_id, DEFAULT_CHANNEL))
                

                ###################################################################################################################################################
                # Below commented part is regarding product 8 Dates which is not requierd currently. It will though will be used in later release of the project. #
                ###################################################################################################################################################

                # # if the product id is for 8 dates product then executing the below portion of code
                # if int(product_id) == DEFAULT_8DATES:
                    
                #     # inserting an extra record for mapping of the product with there parent
                #     insertionQuery = "INSERT INTO `user_sub_products` (`user_product_id`, `user_partner_id`) VALUES(%s, %s)"
                #     # Executing the query
                #     cursor.execute(insertionQuery, (int(user_product_id), int(user_partner_id)))
                    
                #     # selecting the mapping of product with there parent product inserted above
                #     selectionQuery = "SELECT `id` FROM `user_sub_products` WHERE `user_product_id`=%s AND `user_partner_id`=%s"
                #     # Executing the above selection Query
                #     cursor.execute(selectionQuery, (int(user_product_id), int(user_partner_id)))
                    
                #     # fetching the first record data from the cursor
                #     user_sub_products = cursor.fetchone()
                #     # fetching the parent_user_product_id from user_sub_products
                    
                #     user_sub_products_id = user_sub_products[0]
                    
                #     try:
                #         # making an boto 3 client object
                #         invokeLam = make_client()
                        
                #         # preparing the payload for lambda invocation
                #         payload = {"headers":{"Authorization":auth_token, "user_partner_id": None}}
                        
                #         # invoking the lambda function with custom payload
                #         response = invokeLam.invoke(FunctionName= "ProfilesActiveCampaign" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
                #     except:
                #         # If there is any error in above operations, logging the error
                #         logger.error(traceback.format_exc())
                #         return log_err(config[message_by_language]['INVOCATION_ERROR'])
                #         # returning the success json when purchase is for 8 dates
                
                #     return {
                #         'statusCode': 200,
                #         'headers': {
                #             'Access-Control-Allow-Origin': '*',
                #             'Access-Control-Allow-Credentials': 'true'
                #         },
                #         'body': json.dumps({"message": config[message_by_language]['SUCCESS_MESSAGE'],"user_partner_id": user_partner_id,"user_sub_products_id": user_sub_products_id})}

                # else:    
                try:
                    # making an boto 3 client object
                    invokeLam = make_client()
                    
                    # preparing the payload for lambda invocation
                    payload = {"headers":{"Authorization":auth_token, "user_partner_id": None}}
                    
                    # invoking the lambda function with custom payload
                    response = invokeLam.invoke(FunctionName= "ProfilesActiveCampaign" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
                except:
                    # If there is any error in above operations, logging the error
                    logger.error(traceback.format_exc())
                    return log_err(config[message_by_language]['INVOCATION_ERROR'])
                    # returning success json
                return {
                            'statusCode': 200,
                            'headers':{
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                                },
                            'body': json.dumps({"message": config[message_by_language]['SUCCESS_MESSAGE'], "user_product_id" : user_product_id, "is_cancelled" : is_cancelled})
                        }
            else:
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['FAILURE_MESSAGE'], 400)
        except:
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['QUERY_STATUS'], 500)
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
        
if __name__== "__main__":
    handler(None,None)