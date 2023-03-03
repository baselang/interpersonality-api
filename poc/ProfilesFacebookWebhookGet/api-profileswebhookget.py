import json
import logging
import traceback
from datetime import datetime  
from datetime import timedelta

# VERIFY TOKEN
VERIFY_TOKEN = '12345678'

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

def handler(event,context):
    """Function to handle the request for Get Description API."""
    print("test data")
    print(event)
    logger.info("test1")
    logger.info(event["queryStringParameters"])
    logger.info("test2")
    
    logger.info(event)
    #data = event['body']
    data = event['queryStringParameters']
    #data  = json.loads(body)
    logger.info("test3")
    logger.info(data)
    logger.info(data['hub.challenge'])
    logger.info("test4")
    if data['hub.verify_token'] and data['hub.challenge']:
        token = data['hub.verify_token']
        challenge = data['hub.challenge']
    else:
        return log_err("ERROR: hub.verify not available")
    print(token)
    try:
        if VERIFY_TOKEN == str(token):
            return {
                    "body": str(challenge) , 
                    "headers":{
                              'Access-Control-Allow-Origin': '*',
                              'Access-Control-Allow-Credentials': 'true'
                              },
                              "statusCode": 200
                   }
        else:
            return {
                    "body": "incorrect" , 
                    "headers":{
                              'Access-Control-Allow-Origin': '*',
                              'Access-Control-Allow-Credentials': 'true'
                              },
                              "statusCode": 200
                   }
    
    except:
        # If there is any error in above operations, logging the error
        return log_err("ERROR: Cannot connect to database from handler.\n{}".format(
            traceback.format_exc()))

if __name__== "__main__":
    handler(None,None)
