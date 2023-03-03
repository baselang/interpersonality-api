"""

API Module to provide Authentication to each Call after Sign in.

It provides the following functionalities:
1. jwt_verify(): It will verify the token for authentication of a user
2. log_err(): Logging error and returning the JSON response with error message & status code
3. generate_policy(): Function to generate output response
4. handler(): Handling the incoming request with following steps:
- Accepts the token i.e. authorizationToken, from the request header Auth and also methodArn
- Returning the response that the user is verified user to access the api or not

"""
import jwt
import json
from os import environ
import logging
import traceback


SECRET_KEY = environ.get('TOKEN_ACCESS_KEY')

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
# Setting the log level to INFO
logger.setLevel(logging_Level)

logger.info("Cold start complete.")

def log_err(errmsg):
    """Function to log the error messages."""
    return  {
                "statusCode": 403,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def handler(event, context):
    """Function to handle request from all the api and authorize that request"""
    logger.info(event)
    # Accessing data in the event object
    auth_token = event['authorizationToken']
    method_arn = event['methodArn']
    logger.info(auth_token)
    # If auth token is not present giving the response
    if not auth_token:
        return log_err('Unauthorized user')
    
    # Checking that the token is valid or not and giving response
    try:
        principal_id = jwt_verify(auth_token)
        policy = generate_policy(principal_id, 'Allow', method_arn)
        logger.info(policy)
        return policy
    except:
        policy = generate_policy('user', 'Deny', method_arn)
        return policy


def jwt_verify(auth_token):
    """Function to verify the authorization token"""
    payload = jwt.decode(auth_token, SECRET_KEY, options={'require_exp': True})
    return payload['id']


def generate_policy(principal_id, effect, resource):
    """Function to generate output response"""
    return {
        'principalId': principal_id,
        'policyDocument': {
            'Version': '2012-10-17',
            'Statement': [
                {
                    "Action": "execute-api:Invoke",
                    "Effect": effect,
                    "Resource": resource

                }
            ]
           }
        }
        
if __name__== "__main__":
    handler(None,None)