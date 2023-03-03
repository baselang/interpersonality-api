"""
API Module to chargebee cancel payment

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
config.read('chargebeecancelpayment.properties')


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
        print("SITE_KEY",SITE_KEY)
        print("SITE_URL",SITE_URL)
        #data = json.loads(event['body'])
        #subscriptionId = data["subscriptionId"]
        #print("subscriptionId",subscriptionId)
        
        
        # customer = result.customer
        # card = result.card
        # invoice = result.invoice
        # unbilled_charges = result.unbilled_charges
        # credit_notes = result.credit_notes
        
        #print("subscription",subscription)
        # print("customer",customer)
        # print("card",card)
        # print("invoice",invoice)
        # print("unbilled_charges",unbilled_charges)
        # print("credit_notes",credit_notes)


        # configuring chargebee object
        chargebee.configure(SITE_KEY,SITE_URL)
        entries = chargebee.Subscription.list({"customer_id[is]" : CUSTOMER_ID,"plan_id[is]" : PLAN_ID,"status[is]" : "non_renewing"})


        if entries != [] :
            for entry in entries:
                subscription = entry.subscription
                #customer = entry.customer
                #card = entry.card
                print("subscription",subscription.id)

                # cancels the subscription.        
                result = chargebee.Subscription.cancel(subscription.id,{
                    "end_of_term" : "false",
                    "refundable_credits_handling":"schedule_refund",
                    "credit_option_for_current_term_charges":"full"
                    })
                subscription = result.subscription

                # print("customer",customer)
                # print("card",card)
            
            return {
                    'statusCode': 200,
                    'headers':{
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                              },
                    'body': json.dumps({"message":"Payment cancelled successfully."})
                }
        else :
            logger.info(traceback.format_exc())
            return log_err(config[message_by_language]['THERE_IS_NO_PURCHASE_TO_CANCEL_AND_REFUND'], 500)

    except:
        logger.info(traceback.format_exc())
        return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
        
if __name__== "__main__":
    handler(None,None)
