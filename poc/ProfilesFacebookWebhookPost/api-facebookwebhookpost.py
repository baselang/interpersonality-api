import json
import logging
import traceback
from datetime import datetime  
from datetime import timedelta
import requests

# VERIFY TOKEN
FB_API_URL = 'https://graph.facebook.com/v4.0/me/messages'
VERIFY_TOKEN = '12345678'# <paste your verify token here>
PAGE_ACCESS_TOKEN = 'EAAFrspyysZAUBAKHJuDJ66zWqtNFvkiOuxf5RM0QDaydhn5aFAxjxxR0M8BYQVltgv3I2IkQUxQ7YPEdhctVtZCOE9jikZBwSoTjzZBf6YZCluZC7WBxqzsEbH1OtzzvYsWjJeolh3JQrS0TWWBduSwXteZAyssusEMmnY0OfCAynPvBhFdIpBc1bPh8hh7nHtrMirZB9vyBUHm59DoPsKzR'# paste your page access token here>"

# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
# Setting the log level to INFO
logger.setLevel(logging.INFO)

logger.info("Cold start complete.")

def log_err(errmsg):
    """Function to log the error messages."""
    logger.error(errmsg)
    return {"body": errmsg , "headers": {}, "statusCode": 400,
        "isBase64Encoded":"false"}


def send_message(recipient_id, text):
    """Send a response to Facebook"""
    payload = {
        'message': {
            'text': text
        },
        'recipient': {
            'id': recipient_id
        },
        'notification_type': 'regular'
    }

    auth = {
        'access_token': PAGE_ACCESS_TOKEN
    }

    response = requests.post(
        FB_API_URL,
        params=auth,
        json=payload
    )

    logger.info("respond1")
    logger.info(response.json())
    return response.json()
    #return "Hello"

def respond(sender, message):
    """Formulate a response to the user and
    pass it on to a function that sends it."""
    response = message
    logger.info("test101")
    logger.info(response)
    logger.info("test102")
    send_message(sender, response)

def handler(event,context):
    """Function to handle the request for Get Description API."""
    logger.info("test1")
    logger.info(event)
    data = json.loads(event['body'])
    #data = event
    #body
    try:
        my_event = data['entry'][0]
        print("test2")
        print(my_event)
        my_event = my_event['messaging']
        print("test3")
        print(my_event)
        for x in my_event:
            text = x['message']
            sender_id = x['sender']['id']
            respond(sender_id, text)

        return {
                "body": "OK" ,
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                          },
                "statusCode": 200
               }
    
    except:
        # If there is any error in above operations, logging the error
        return log_err("ERROR: Error in getting my_event data.".format(traceback.format_exc()))

if __name__== "__main__":
    handler(None,None)
