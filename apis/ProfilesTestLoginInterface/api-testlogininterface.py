
from os import environ
import configparser
import logging
import traceback
import json


# Login Token and Credentials
LOGIN_TOKEN = environ.get('LOGIN_TOKEN')
LOGIN_USER_ID = environ.get('LOGIN_USER_ID')
LOGIN_PASSWORD = environ.get('LOGIN_PASSWORD')


# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('testlogininterface.properties', encoding = "ISO-8859-1")

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# getting message variable
message_by_language = "165_MESSAGES"

# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
# Setting the log level to INFO
logger.setLevel(logging_Level)
logger.info("Cold start complete.")

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

def handler(event, context):
	try:
		logger.info(event)
		body = json.loads(event['body'])
		login_user_id =  body['login_user_id']
		login_password =  body['login_password']

		if login_user_id == LOGIN_USER_ID and login_password == LOGIN_PASSWORD:
			# Returning Login_Token to therequest.
			return {
						'statusCode': 200,
						'headers':  {
									   'Access-Control-Allow-Origin': '*',
										'Access-Control-Allow-Credentials': 'true'
									},
						'body': json.dumps({"test_token": LOGIN_TOKEN})
						}
			
		else:
			# returning the error when there is some error in above try block
			logger.error(traceback.format_exc())
			return log_err(config[message_by_language]['USER_STATUS'], 400)

	
	except:
		# returning the error when there is some error in above try block
		logger.error(traceback.format_exc())
		return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)

	