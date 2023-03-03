"""
API to act as webhook for chargebee card expiry notification.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- Chargebee will hit the api whenever an even occurs at chargebee end.
 - Based on that event a notification entry will be feeded for the respected user in database.
"""

import chargebee
import json
import logging
from os import environ
import pymysql
import traceback
import configparser
import json

# getting message variable
message_by_language = "165_MESSAGES"

# Environment required for chargebee
SITE_KEY = environ.get('SITE_KEY')
SITE_URL = environ.get('SITE_URL')

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get("ENDPOINT")
port = environ.get("PORT")
dbuser = environ.get("DBUSER")
password = environ.get("DBPASSWORD")
database = environ.get("DATABASE")

# variables required for database use
notification_type = int(environ.get("NOTIFICATION_TYPE"))
EVENT_TYPE = environ.get('EVENT_TYPE')
update_link = environ.get('UPDATE_LINK')

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('webhooknotification.properties', encoding = "ISO-8859-1")

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


def handler(event, context):
    try:
        # Making the DB connection
        cnx = make_connection()
        
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()

        logger.info(event)

        body = json.loads(event['body'])
        event_type = body['event_type']

        # Checking Weather event_type is card expiry reminder or not.
        if event_type == EVENT_TYPE:

            customer_id = body['content']['customer']['id']
            card_number = body['content']['card']['last4']

            # Fetching user Data on the basis of customer_id
            selectionQuery = "SELECT `id`, `user_id` FROM `users` WHERE `customer_id` = %s"
            cursor.execute(selectionQuery, (customer_id))
            
            result_list = []

            for result in cursor: result_list.append(result)
            if len(result_list) == 0:
                return {
                    'statusCode': 400, 
                        'headers': {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                            },
                        'body': json.dumps({"message": config[message_by_language]['USER_NOT_EXIST'].format(customer_id)})
                    }
            rid = result_list[0][0]
            user_id = result_list[0][1]

            

            json_data = json.dumps({'card_number': card_number, 'profile_link': update_link})

            # Inserting Notification entry into database.
            insertQuery = "INSERT INTO `notifications` (`json`, `user_id`, `rid`, `notification_type`) VALUES (%s, %s, %s, %s)"
            cursor.execute(insertQuery, (json_data, user_id, int(rid), notification_type))

            return {'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true',
                    },
                'body': json.dumps({"message": config[message_by_language]['SUCCESS_STATUS'], "customer_id": customer_id, 'card_number': card_number})
            }

        
        else:
            logger.info(traceback.format_exc())
            # If there is any error in above operations, logging the error
            return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)


    except:
        logger.info(traceback.format_exc())
        # If there is any error in above operations, logging the error
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)


if __name__ == '__main__':
    print(handler(None, None))
