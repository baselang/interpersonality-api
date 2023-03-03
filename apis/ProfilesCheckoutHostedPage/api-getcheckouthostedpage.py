"""
API for getting Checkout Hosted Page for user to make product purchase.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. make_client(): Function to make boto3 aws client
5. handler(): Handling the incoming request with following steps:
- Fetching data from request
- creating chargebee customer for an user if he does not have any customer_id
- creating a new checkout hosted page for current user for purchase of the product selected by user
- Returning the JSON response with success status code with the required data
"""
import boto3
import chargebee
import pymysql
import jwt
import logging
import json
from os import environ
import traceback
import configparser
import httpagentparser

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('getcheckouthostedpage.properties', encoding = "ISO-8859-1")

# getting message variable
message_by_language = "165_MESSAGES"
DEFAULT_LANGUAGE = "165"
transaction_type = environ.get('TRANSACTION_TYPE')

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port = environ.get('PORT')
dbuser = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

# Environment required for chargebee
SITE_KEY = environ.get('SITE_KEY')
SITE_URL = environ.get('SITE_URL')

# default variable
DEFAULT_CHANNEL = int(environ.get('DEFAULT_CHANNEL'))

# aws cridentials required for creating boto3 client object
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')

# variables required for Checkout hosted page
is_cancelled = int(environ.get('IS_CANCELLED'))

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# Getting the logger to log the messages for DEBUG_PROPAGATE_EXCEPTIONS = Falseing purposes
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


def handler(event, context):
    """Function to handle the request for Chargebee Checkout and First payment API."""
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
        # getting variable from request
        product_id = event['headers']['product_id']
        auth_token = event['headers']['Authorization']
        user_agent = event['headers']['User-Agent']

        # configuring chargebee object
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
            # getting plan_id of the current product
            selectionQuery = "SELECT `plan_id` FROM `products` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (int(product_id)))

            plan_list = []
            # fetching result from the cursor
            for result in cursor: plan_list.append(result)

            # fetching plan_id for user
            plan_id = plan_list[0][0]
            
            logger.info(key)

            # getting chargebee_id of the user
            selectionQuery = "SELECT `language_id`, `customer_id`, CAST(AES_DECRYPT(`firstname`,%s) AS CHAR), CAST(AES_DECRYPT(`lastname`,%s) AS CHAR), CAST(AES_DECRYPT(`primary_email`,%s) AS CHAR), `ac_contact_id` FROM `users` WHERE `id` = %s"
            # Executing the Query
            cursor.execute(selectionQuery, (key, key, key, int(rid)))

            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)

            # getting the data from the result_list
            language_id = result_list[0][0]
            message_by_language = str(language_id) + "_MESSAGES"
            customer_id = result_list[0][1]
            first_name = result_list[0][2]
            last_name = result_list[0][3]
            email = result_list[0][4]
            ac_contact_id = result_list[0][5]

            selectionQuery = "SELECT `code` FROM `language` WHERE `id`=%s"
            cursor.execute(selectionQuery, (int(language_id)))
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            language_code = result_list[0][0]

            if customer_id == None:
                # creating a Chargebee customer for current user
                result = chargebee.Customer.create({
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email,
                    "locale" : language_code,
                })
                # getting customer id of the customer created above
                customer_id = str(result.customer.id)

                # inserting customer_id into users account
                updationQuery = "UPDATE `users` SET `customer_id` = %s WHERE `id`=%s"
                # Executing the Query

                cursor.execute(updationQuery, (customer_id, rid))
        except:
            logger.info(traceback.format_exc())
            # If there is any error in above operations, logging the error
            return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
    except:
        logger.info(traceback.format_exc())
        # If there is any error in above operations, logging the error
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)

    try:
        # getting checkout hosted page data
        selectionQuery = "SELECT * FROM `user_product` WHERE `rid`=%s and `status` in ('purchased')"
        cursor.execute(selectionQuery, int(rid))
        data = cursor.fetchone()

        logger.info(data)


        if data == None:

            logger.info(event)
            # configuring chargebee object
            chargebee.configure(SITE_KEY, SITE_URL)
            result = chargebee.HostedPage.checkout_new({
                "subscription": {
                    "plan_id": plan_id
                },
                "customer": {
                    "id": customer_id
                }
            })

            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
                'body': json.dumps({"site": SITE_URL, "hosted_page": json.loads(str(result.hosted_page))})
            }
        else:
            logger.info(event)

            # configuring chargebee object
            chargebee.configure(SITE_KEY, SITE_URL)
            result = chargebee.Subscription.create_for_customer(customer_id, {
                "plan_id": plan_id
            })
            subscription = result.subscription



            insertQuery = "INSERT INTO `user_product` SET `product_id`= %s, `status`=%s, `subscription_id` =%s ,`rid`=%s"
            invoice = result.invoice

            if invoice.status == "paid":
                cursor.execute(insertQuery, (int(product_id), "purchased", str(subscription.id), int(rid)))
                
                selectionQuery = "SELECT `id`, `transaction_date` from `user_product` WHERE `subscription_id`=%s"
                cursor.execute(selectionQuery, str(subscription.id))
                data = cursor.fetchone()
                user_product_id = data[0]
                transaction_timestamp = data[1]
                
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
                cursor.execute(insertionQuery, (int(product_id), int(transaction_type), currency_code, int(rid), user_id, transaction_timestamp, amount, os_name, browser_name, str(subscription.id), customer_id, DEFAULT_CHANNEL))
                
                
                try:
                    # making an boto 3 client object
                    invokeLam = make_client()
                    
                    # preparing the payload for lambda invocation
                    payload = {"headers":{"Authorization":auth_token, "user_partner_id": None}}
                    
                    if ac_contact_id == None:
                        # invoking the lambda function with custom payload
                        response = invokeLam.invoke(FunctionName= "ProfilesActiveCampaign" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))

                    else:
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
                    'body': json.dumps(
                        {"message": config[message_by_language]['SUCCESS_MESSAGE'], "user_product_id": user_product_id, "is_cancelled" : is_cancelled})
                }
            else:
                return {
                    'statusCode': 200,
                    'headers': {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Credentials': 'true'
                    },
                    'body': json.dumps({"message": config[message_by_language]['FAILURE_MESSAGE']})
                }
    except:
        logger.info(traceback.format_exc())
        return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
        
    finally:
        try:
            # Finally, clean up the connection
            cursor.close()
            cnx.close()
        except:
            pass


