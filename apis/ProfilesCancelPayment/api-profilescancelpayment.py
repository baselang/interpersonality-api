"""
API Module to chargebee cancel payment

It provides the following functionalities:
1. log_err(): Logging error and returning the JSON response with error message & status code
2. make_connection(): Connecting to the Database using connection details received through environment variables
3. make_client(): Function to make boto3 aws client
4. jwt_verify(): verifying token and fetching data from the jwt token sent by use
5. handler(): Handling the incoming request with following steps:
- Fetching data required for api
- returning the success json with json data
"""

import boto3
import chargebee
import json
import logging
import traceback
import pymysql
import jwt
from os import environ
import configparser
from datetime import datetime
import httpagentparser

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('profilescancelpayment.properties', encoding = "ISO-8859-1")

# Environment required for chargebee
SITE_KEY = environ.get('SITE_KEY')
SITE_URL = environ.get('SITE_URL')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port = int(environ.get('PORT'))
dbuser = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# aws cridentials required for creating boto3 client object
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')

# parameter required
TIME_DIFF = int(environ.get('TIME_DIFF'))
transaction_type = environ.get('TRANSACTION_TYPE')
DEFAULT_CHANNEL = int(environ.get('DEFAULT_CHANNEL'))
DEFAULT_LANGUAGE = "165"

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# Getting the logger to log the messages for debugging purposes
logger = logging.getLogger()

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
    return {
        "statusCode": status_code,
        "body": json.dumps({"message": errmsg}),
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': 'true'},
        "isBase64Encoded": "false"
    }

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

def jwt_verify(auth_token):
    """Function to verify the authorization token"""
    # decoding the authorization token provided by user
    payload = jwt.decode(auth_token, SECRET_KEY, options={'require_exp': True})

    # setting the required values in return
    rid = int(payload['id'])
    user_id = payload['user_id']
    language_id = payload['language_id']
    return rid, user_id, language_id


def handler(event, context):
    """Function to handle the request for notifications API"""
    message_by_language = "165_MESSAGES"
    try:
        logger.info(event)
        
        # getting variable from request
        user_product_id = event['headers']['product_id']
        auth_token = event['headers']['Authorization']
        user_agent = event['headers']['User-Agent']
        chargebee.configure(SITE_KEY, SITE_URL)
    except:
        logger.info(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
    
    
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
        cnx = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        
        try:
            # Query for getting current language of the user
            selectionQuery = "SELECT `language_id`, `customer_id`, `ac_contact_id` FROM `users` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (rid))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            # fetching parameters from the result_list
            language_id = result_list[0][0]
            customer_id = result_list[0][1]
            ac_contact_id = result_list[0][2]
            
            message_by_language = str(language_id) + "_MESSAGES"
            
            # getting transaction_date of the current product
            selectionQuery = "SELECT `transaction_date`, `subscription_id`, `product_id` FROM `user_product` WHERE `id`=%s AND `rid`=%s"

            # Executing the Query
            cursor.execute(selectionQuery, (int(user_product_id), int(rid)))

            date_list = []
            # fetching result from the cursor
            for result in cursor: date_list.append(result)
            logger.info(rid)
            logger.info(date_list)

            purchase_time = date_list[0][0]
            subscription_id = date_list[0][1]
            p_id = date_list[0][2]

            now = datetime.now()
            diff = now - purchase_time
            diff = diff.seconds / 60


            if diff <= TIME_DIFF:

                # getting plan_id of the current product
                selectionQuery = "SELECT `partner_rid` FROM `user_partner_products` WHERE `user_product_id`=%s"

                # Executing the Query
                cursor.execute(selectionQuery, (int(user_product_id)))


                date_list = cursor.fetchone()

                if date_list == None:

                    # getting plan_id of the current product
                    selectionQuery = "SELECT `plan_id` FROM `products` WHERE `id`=%s"

                    # Executing the Query
                    cursor.execute(selectionQuery, (int(p_id)))

                    date_list = []
                    # fetching result from the cursor
                    for result in cursor: date_list.append(result)

                    PLAN_ID = date_list[0][0]


                    # cancels the subscription.
                    result = chargebee.Subscription.cancel(subscription_id, {
                        "end_of_term": "false",
                        "refundable_credits_handling": "schedule_refund",
                        "credit_option_for_current_term_charges": "full"
                    })
                    subscription = result.subscription

                    deleteQuery = "DELETE FROM `user_product` WHERE `subscription_id`= %s AND `rid`=%s"
                    cursor.execute(deleteQuery, (subscription_id, int(rid)))
                    
                    if language_id==int(DEFAULT_LANGUAGE):
                        # Executing the following code when language_id is default
                        
                        # Query for getting all the guides details from the products
                        selectionQuery = "SELECT `currency_code`, `amount`  FROM `products` WHERE `id`=%s"
                        # Executing the Query
                        cursor.execute(selectionQuery, (int(p_id)))
                    else:
                        # Executing the following code when language_id is default
                        
                        # Query for getting all the guides details from the products
                        selectionQuery = "SELECT `currency_code`, `amount`  FROM `products_translations` WHERE `product_id`=%s AND `language_id`=%s"
                        # Executing the Query
                        cursor.execute(selectionQuery, (int(p_id), int(language_id)))
                        
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
                    insertionQuery = "INSERT INTO `user_transactions` (`product_id`, `transaction_type`, `currency_code`, `rid`, `user_id`, `transaction_timestamp`, `amount`, `os_name`, `browser_name`, `subscription_id`, `customer_id`, `acquisition_channel`) VALUES (%s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s)"
                    # Executin the query
                    cursor.execute(insertionQuery, (int(p_id), int(transaction_type), currency_code, int(rid), user_id, amount, os_name, browser_name, subscription_id, customer_id, DEFAULT_CHANNEL))
                    

                    if ac_contact_id == None:
                        pass
                    else:
                        try:
                            # making an boto 3 client object
                            invokeLam = make_client()
                            
                            # preparing the payload for lambda invocation
                            payload = {"headers":{"Authorization":auth_token, "user_partner_id": None}}
                            
                            # invoking the lambda function with custom payload
                            response = invokeLam.invoke(FunctionName= "ProfilesActiveCampaignUpdate" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
                        except:
                            # If there is any error in above operations, logging the error
                            logger.error(traceback.format_exc())
                            return log_err(config[message_by_language]['INVOCATION_ERROR'])
                    
                    return {
                        'statusCode': 200,
                        'headers': {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                        },
                        'body': json.dumps({"message": config[message_by_language]['SUCCESS_STATUS']})
                    }
                else:
                    logger.info(traceback.format_exc())
                    return {'statusCode': 400,
                            'headers': {
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                            },
                            'body': json.dumps({"message": config[message_by_language]['ALREADY_SELECTED_PARTNER']})
                            }
            else:
                logger.info(traceback.format_exc())
                return {'statusCode': 400,
                        'headers': {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                        },
                        'body': json.dumps({"message": config[message_by_language]['EXPIRY_STATUS']})
                    }

        except:
            logger.info(traceback.format_exc())
            # If there is any error in above operations, logging the error
            return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)

    except:
        logger.info(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)
    finally:
        try:
            # Finally, clean up the connection
            cursor.close()
            cnx.close()
        except:
            pass


