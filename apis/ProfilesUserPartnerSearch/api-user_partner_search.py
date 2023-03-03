"""
API for searching the user partner according to the provided interpersonality link

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching user_id from the data from request
- Fetching the required data from the user_id
- Returning the JSON response with success status code with the required data
"""

import pymysql
import jwt
import logging
import json
from os import environ
import traceback
import configparser

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('user_partner_search.properties', encoding = "ISO-8859-1")

# getting message variable
message_by_language = "165_MESSAGES"

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port = environ.get('PORT')
dbuser = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

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
    """Function to handle the request for Get Big5 API."""
    global message_by_language
    logger.info(event)
    try:
        # getting variable from request
        body = json.loads(event['body'])
        link = body['link']
        searched_user_id = link.split("/")[-1]
        auth_token = event['headers']['Authorization']
        purchase_id = int(event['headers']['purchase_id'])
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
            selectionQuery = "SELECT `language_id` FROM `users` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (rid))

            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)

            # getting current language_id of the user
            language_id = int(result_list[0][0])
            message_by_language = str(language_id) + "_MESSAGES"

            # getting plan_id of the current product
            selectionQuery = "SELECT `product_id` FROM `user_product` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (int(purchase_id)))

            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)

            # fetching the product_id
            product_id = int(result_list[0][0])

            # getting plan_id of the current product
            selectionQuery = "SELECT `id`, CAST(AES_DECRYPT(`name`,%s) AS CHAR), `picture_url`, CAST(AES_DECRYPT(`firstname`,%s) AS CHAR), (SELECT `is_privacy_consent` FROM `user_permissions` where user_id = %s) FROM `users` WHERE `user_id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (key, key, searched_user_id, searched_user_id))

            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)

            # fetching partner information
            partner_rid = result_list[0][0]
            partner_name = result_list[0][1]
            partner_image = result_list[0][2]
            partner_firstname = result_list[0][3]
            partner_privacy_consent = result_list[0][4]

            logger.info(key)


            if user_id == searched_user_id:
                return {
                                'statusCode': 200,
                                'headers': {
                                    'Access-Control-Allow-Origin': '*',
                                    'Access-Control-Allow-Credentials': 'true'
                                },
                                'body': json.dumps({"message": config[message_by_language]['NO_PARTNER'], "partner_name": partner_name, "partner_picture_url": partner_image})
                            }


            else:

                # getting plan_id of the current product
                selectionQuery = "SELECT count(*) FROM `user_partner_products` WHERE `user_rid`=%s AND `partner_rid`=%s AND `product_id`=%s"
                # Executing the Query
                cursor.execute(selectionQuery, (int(rid), int(partner_rid), product_id))

                result_list = []
                # fetching result from the cursor
                for result in cursor: result_list.append(result)

                count = result_list[0][0]

                if int(count)>0 :

                    msg = str(config[message_by_language]['USER_EXISTENCE_STATUS_PRE']).format(str(partner_firstname))
                    return {
                                'statusCode': 200,
                                'headers': {
                                    'Access-Control-Allow-Origin': '*',
                                    'Access-Control-Allow-Credentials': 'true'
                                },
                                'body': json.dumps({"message": msg, "partner_name": partner_name, "partner_picture_url": partner_image})
                            }
                else:
                    return {
                        'statusCode': 200,
                        'headers': {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                        },
                        'body': json.dumps({"partner_name": partner_name, "partner_picture_url": partner_image, "partner_user_id": searched_user_id, "partner_privacy_consent":partner_privacy_consent})
                    }
        except:
            logger.info(traceback.format_exc())
            # If there is any error in above operations, logging the error
            return log_err(config[message_by_language]['LINK_VALIDATION_STATUS'], 400)
    except:
        logger.info(traceback.format_exc())
        # If there is any error in above operations, logging the error
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)
    finally:
        try:
            # Finally, clean up the connection
            cursor.close()
            cnx.close()
        except:
            pass



