"""
API Module to chargebee one click payment

It provides the following functionalities:
1. log_err(): Logging error and returning the JSON response with error message & status code
2. handler(): Handling the incoming request with following steps:
- Fetching data required for api
- returning the success json with json data

"""

import chargebee
import json
import logging
import traceback
# import requests
from os import environ
import configparser
# from datetime import datetime

# # reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('chargebeeoneclickpayment.properties')


# Environment required for chargebee
SITE_KEY = environ.get('SITE_KEY')
SITE_URL = environ.get('SITE_URL')
PLAN_ID = environ.get('PLAN_ID')
CUSTOMER_ID = environ.get('CUSTOMER_ID')

# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
# Setting the log level to INFO
logger.setLevel(logging.INFO)

logger.info("Cold start complete.")

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

def handler(event,context):
    """Function to handle the request for notifications API"""
    message_by_language = "165_MESSAGES"
    try:
        logger.info(event)
        #data = json.loads(event['body'])
       
        # configuring chargebee object
        chargebee.configure(SITE_KEY,SITE_URL)
        result = chargebee.Subscription.create_for_customer(CUSTOMER_ID,{
            "plan_id" : PLAN_ID
            # ,
            # "trial_end": 0
            # ,
            # "start_date" : 2147483647,
            # "shipping_address" : {
            #     "first_name" : "Hareesh",
            #     "last_name" : "Soni",
            #     "company" : "chargebee"
            #     }
             })
        subscription = result.subscription
        # customer = result.customer
        # card = result.card
        # invoice = result.invoice
        # unbilled_charges = result.unbilled_charges

        print("subscription",subscription)
        # print("customer",customer)
        # print("card",card)
        # print("invoice",invoice)
        # print("unbilled_charges",unbilled_charges)

    
        return {
                    'statusCode': 200,
                    'headers':{
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                              },
                    'body': json.dumps({"message":"Payment done successfully.","subscriptionId":subscription.id})
                }
    except:
        logger.info(traceback.format_exc())
        return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
        
if __name__== "__main__":
    handler(None,None)
