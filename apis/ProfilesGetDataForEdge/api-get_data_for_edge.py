import json
import logging
import traceback
import pymysql
from os import environ
import configparser

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port = environ.get('PORT')
dbuser = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')
key = environ.get('DB_ENCRYPTION_KEY')

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))

# Getting the logger to log the messages for debugging purposes
logger = logging.getLogger()
# Setting the log level to INFO
logger.setLevel(logging_Level)

logger.info("Cold start complete.")

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('get_data_for_edge.properties', encoding = "ISO-8859-1")

message_by_language = "165_MESSAGES"


def make_connection():
    """Function to make the database connection."""
    return pymysql.connect(host=endpoint, user=dbuser, passwd=password,
        port=int(port), db=database, autocommit=True)


def log_err(errmsg):
    """Function to log the error messages."""
    return  {
                "statusCode": 500,
                "body": json.dumps({"message":errmsg}) ,
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'},
                "isBase64Encoded":"false"
            }


def handler(event, context):
    try:
        logger.info(event)
        # checking that the following event call is from lambda warmer or not
        if event['source'] == "lambda_warmer":
            logger.info("lambda warmed")
            # returning the success json
            return {
                       'status_code': 200,
                       'body': {"message": "lambda warmed"}
                   }
    except:
        # If there is any error in above operations
        pass

    try:
        # Making the DB connection
        cnx = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()

        # Fetching event data from request event object
        user_id = event['user_id']

        language_id = None
        first_name = None

        selectionQuery = "SELECT `language_id`, cast(AES_DECRYPT(`firstname`,%s) as char) FROM `users` WHERE `user_id`=%s"
        # Executing the Query
        cursor.execute(selectionQuery, (key, user_id))

        result_list = []
        for result in cursor:
            result_list.append(result)

        if result_list:
            language_id = int(result_list[0][0])
            first_name = result_list[0][1]

        return {
            'statusCode': 200,
            'body': json.dumps({'language_id':language_id, 'firstname': first_name})
        }

    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'])


if __name__== "__main__":
    handler(None,None)
