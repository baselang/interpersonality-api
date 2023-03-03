"""
API for getting Invoice pdf.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching data from request
- getting invoice pdf from Chargebee using invoice id
- Returning the JSON response with success status code with the required data
"""

import chargebee
import jwt
import logging
import json
from os import environ
import traceback
import configparser
from pyDes import *

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('getinvoiceurl.properties', encoding = "ISO-8859-1")

# getting message variable
message_by_language = "165_MESSAGES"

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')
INVOICE_SECRET = environ.get('INVOICE_SECRET')

# Environment required for chargebee

SITE_KEY = environ.get('SITE_KEY')
SITE_URL = environ.get('SITE_URL')

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
    rid = int(payload['id'])
    user_id = payload['user_id']
    language_id = payload['language_id']
    return rid, user_id, language_id

def handler(event,context):
    """Function to handle the request for Get Big5 API."""
    global message_by_language
    logger.info(event)
    try:
        # getting variable from request
        invoice_id = event['headers']['invoice_id']
        invoice_status = event['headers']['invoice_status']
        auth_token = event['headers']['Authorization']

        # configuring chargebee object
        chargebee.configure(SITE_KEY,SITE_URL)
    except:
        logger.info(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
        
    # decrypting the encrypted data
    try:
        ciphertext = bytes.fromhex(invoice_id)
        invoice_id = (triple_des(INVOICE_SECRET).decrypt(ciphertext, padmode=2)).decode('utf-8')
    except:
        logger.info(traceback.format_exc())
        return log_err (config[message_by_language]['INVALID_INVOICE'], 500)
    
    try:
        # verifying that the user is authorized or not to see this api's data
        rid, user_id, language_id = jwt_verify(auth_token)
        message_by_language = str(language_id) + "_MESSAGES"
    except:
        # if user does not have valid authorization
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['UNAUTHORIZED'], 403)
        
    try:
        # fetching invoice as pdf
        if invoice_status == "Paid":
            result = chargebee.Invoice.pdf(invoice_id)

        elif invoice_status == "Refunded":
            result = chargebee.CreditNote.pdf(invoice_id)
        
        # returning the success json with the required data
        return  {
                    'statusCode': 200,
                    'headers': {
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                               },
                    'body': json.dumps({"invoice_url":json.loads(str(result))['download']['download_url']})
                }
    except:
        logger.info(traceback.format_exc())
        return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)