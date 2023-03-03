"""
Lambda Function to invoke all other lambda function and keep them warm or function to implement warm start on lambda function.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- creating a aws client
- invoking the lambda functions for warm start with custom payload
- Returning the JSON response with success status code

"""
import json
import logging
import traceback
import configparser
import boto3
from os import environ

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('lambda_warmer.properties', encoding = "ISO-8859-1")

# aws cridentials required for creating boto3 client object
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
# Setting the log level to INFO
logger.setLevel(logging_Level)

logger.info("Cold start complete.")
            
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

def handler(event,context):
    """Function to handle the request for Lambda Warmer api"""
    try:
        # List of functions which need to be invoked for warm start
        functions_to_invoke = ["ProfilesGetBig5","ProfilesPostAnswers","ProfilesUpdateUsersAnswers","ProfilesGetLanguage","ProfilesActiveNotificationsCount","ProfilesMysteryButton","ProfilesGetUserProfile","ProfilesFacebookSignIn","ProfilesFacebookSignUp","ProfilesSignUp","ProfilesCheckoutHostedPage","ProfilesGenerateUserProfileReport","ProfilesSignIn","ProfilesTermsAndConditionCheck","ProfilesInterpersonal","ProfilesGetProducts", "ProfilesProductSalesDetails","ProfilesGetUserBasicInfo","ProfilesGenerateInterpersonalReport","ProfilesMakeProductPayment","ProfilesGenerateUserGuidesReport","ProfilesProductSalesDetails","ProfilesNotifications", "ProfilesGetDataForEdge"]
        
        # making an boto 3 client object
        invokeLam = make_client()
        
        # creating the payload which need to be sent
        payload = {"source":"lambda_warmer"}
        
        # Invoking the functions that are listed above
        for funct in functions_to_invoke:
            try:
                # invoking the lambda function with custom payload
                response = invokeLam.invoke(FunctionName=funct + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
                logger.info(funct)
            except:
                # if there is any error in invocation of lambda function
                logger.info(funct)
                logger.error(traceback.format_exc())
                return log_err(config['MESSAGES']['INVOCATION_ERROR'])
           
        # returning the success json after warming all the functions
        logger.info("All Lambdas warmed")   
        return {
                    'status_code':200,
                    'body':{"message":"All Lambdas warmed"}
                }
    except:
        # if there is any error in above code
        logger.error(traceback.format_exc())
        return log_err(config['MESSAGES']['INTERNAL_ERROR'])
        
if __name__== "__main__":
    handler(None,None)
    