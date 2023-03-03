"""
API Module to add facebook tags in request for the home and profile page.

It provides the following functionalities:
1. log_err(): Logging error and returning the JSON response with error message & status code
2. make_connection(): To prepare a connection object for pymysql
3.get_details_for_og_tags(): Function for get the details of OG Tags and return the title, description, image URL on home and profile for user  
4. handler(): Handling the incoming request with following steps:
- fetching the data from event and fetching the required content from it
- checking that the url and fetching the page route from url
- checking that the url is one user_id or two user_ids in user home page case and checking url is one user_id in case of user profile 
- if has one user_id and than sending the default static image in case of user home and on user profile case sending the user profile image 
- if have two argument than checking that similarity image for these user_ids exist or not if not than generate them and then sending in the content
- checking that image for user_id is present on S3 or not and if not then generating it
- creating the response by replacing the variables that are required
- Returning the success json

"""

import json
import logging
import traceback
import configparser
import boto3
from botocore.client import Config
import botocore

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('meta_tag_edge.properties', encoding = "ISO-8859-1")

environment_variables = configparser.ConfigParser()
environment_variables.read('environment_variables.properties')

env_var = "ENVIRONMENT"
DEFAULT = "DEFAULT"
DEFAULT_TITLE = "DEFAULT_TITLE"
DEFAULT_DESCRIPTION = "DEFAULT_DESCRIPTION"
HTML_CONTENT = "HTML_CONTENT"
HTML_CONTENT_WITH_OG_TAG = "HTML_CONTENT_WITH_OG_TAG"

# Getting environment variables
APP_ID = environment_variables[env_var]['APP_ID']

# aws credentials required for creating boto3 client object
AWS_REGION = environment_variables[env_var]['REGION']
AWS_ACCESS_KEY = environment_variables[env_var]['ACCESS_KEY_ID']
AWS_SECRET = environment_variables[env_var]['SECRET_ACCESS_KEY']
ENVIRONMENT_TYPE = environment_variables[env_var]['ENVIRONMENT_TYPE']

# Variables related to s3 bucket
SUB_FOLDER = environment_variables[env_var]['SUB_FOLDER']
PROFILE_SUB_FOLDER = environment_variables[env_var]['PROFILE_SUB_FOLDER']
BUCKET_URL = environment_variables[env_var]['BUCKET_URL']
DEFAULT_IMAGE_ENGLISH = environment_variables[env_var]['DEFAULT_IMAGE_ENGLISH']
DEFAULT_IMAGE_SPANISH = environment_variables[env_var]['DEFAULT_IMAGE_SPANISH']

# Logger key
logging_Level = int(environment_variables[env_var]['LOGGING_LEVEL'])
# Getting the logger to log the messages for debugging purposes
logger = logging.getLogger()

message_by_language = "165_MESSAGES"

# Setting the log level to INFO
logger.setLevel(logging_Level)

logger.info("Cold start complete.")


def log_err(errmsg, status_code):
    """Function to log the error messages."""
    return{
                "statusCode": status_code,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }


def make_client():
    """Making a boto3 aws client to perform invoking of functions"""

    # creating an aws client object by providing different credentials
    invokeLam = boto3.client(
        "lambda",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET
    )
    # returning the object
    return invokeLam


def get_details_for_og_tags(uri, route, language_code, message_by_language):
    """Function for get the details of OG Tags and return the title, description, image URL on home and profile for user"""   
    try:
        # code to fetch user_id from the uri
        route = "/"+route+"/"
        user_list = uri.split(route)
        user_list = user_list[-1]
        user_list = user_list.split("/")
        if len(user_list) == 2:
            user_id_1 = user_list[-2]
            user_id_2 = user_list[-1]

        else:
            user_id_1 = user_list[-1]
            user_id_2 = ""

        if language_code != 165:
            message_by_language = str(language_code) + "_MESSAGES"

        logger.info("user_id_list :::::::::\n")
        logger.info(user_list)

        # check for profile in route
        if route == config['OG_URI']['PROFILE']:

            title = config[message_by_language]['USER_PROFILE_TITLE']
            description = config[message_by_language]['USER_PROFILE_DESCRIPTION']

            user_id = user_id_1

            # creating payload for invoking the Lambda function
            payload = {'user_id': user_id}
            try:
                invokeLam = make_client()
                response = invokeLam.invoke(FunctionName="ProfilesGetDataForEdge" + ENVIRONMENT_TYPE,
                                            InvocationType="RequestResponse",
                                            Payload=json.dumps(payload))
            except:
                # logging the error if there is som problem in invocation
                logger.error(traceback.format_exc())
                return log_err(config[message_by_language]['INVOCATION_ERROR'])

            logger.info("RequestResponse :::::::::\n")
            logger.info(response)

            response = json.loads(response['Payload'].read().decode("utf-8"))
            res = json.loads(response['body'])

            first_name = res['firstname']
            language_id = res['language_id']

            logger.info("FirstName :::::::::::::::::")
            logger.info(first_name)

            # if the first name or language_id returned is None then returning the error
            if first_name is None or language_id is None:
                return log_err((config[message_by_language]['INVALID_USER_ID']).format(user_id), 500)

            # if there is no "s" at last of the name and language is english then adding "’s" in the name inside the title
            if language_code == 165:
                if first_name[-1] != 's':
                    title = title.replace("NAME", first_name + "’s")
                else:
                    title = title.replace("NAME", first_name + "’")
                description = description.replace("NAME", first_name)
            else:
                title = title.replace("NAME", first_name)
                description = description.replace("NAME", first_name)

                logger.info("title ::::::::::\n"+title)
                logger.info("description :::::::::\n"+description)

            # getting the image_url
            image_url = BUCKET_URL + PROFILE_SUB_FOLDER + str(language_id) + "_pro_" + str(user_id) + ".png"
        else:
            logger.info("user_id_1 :::::::::\n" + user_id_1)
            logger.info("user_id_2 :::::::::\n" + user_id_2)

            # Query for getting current language of the user
            title = config[message_by_language]['USER_HOME_TITLE']
            description = config[message_by_language]['USER_HOME_DESCRIPTION']

            if user_id_2 != "":
                # getting the image_url
                image_url = BUCKET_URL + SUB_FOLDER + str(language_code)+"_sim_sco_" +str(user_id_1)+ "_" + str(user_id_2) + ".png"
                return title, description, image_url
            # getting the image_url
            image_url = ""
        return title, description, image_url

    except:
        # if above code fails than returning the json
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)


def handler(event,context):
    """Function to handle the request for Lambda Edge for meta tag API"""
    logger.info(event)
    message_by_language = "165_MESSAGES"
    # checking that the following event call is from lambda warmer or not        
    try:
        # Fetching event data from request event object
        uri = event['Records'][0]['cf']['request']['uri']
        logger.info(uri)
        
        # Getting site URL from properties file
        SITE_URL = environment_variables[env_var]['SITE_URL'] + uri
        CANONICAL_URL_VALUE = environment_variables[env_var]['SITE_URL'] + uri
        logger.info(SITE_URL)
        
        if uri.find('.') != -1:
            request = event['Records'][0]['cf']['request']
            logger.info("####Redirected to S3 bucket to serve the static content")
            return request 

        # code for fetch the page route from url
        route_uri = uri.split('/')
        route_uri.pop(0)
        
        page_route = route_uri[0]
        
        if page_route == "es":
            language_code = 245
            locale = "es_ES"
            IMAGE_URL = DEFAULT_IMAGE_SPANISH
            message_by_language = str(language_code) + "_MESSAGES"
            href_uri = uri.split("/es")
            href_uri = href_uri[-1]
            href_uri = href_uri
            
            if len(route_uri) >=2:
                page_route = route_uri[1]
            else:
                page_route = ''
        else:
            language_code = 165
            locale = "en_US"
            IMAGE_URL = DEFAULT_IMAGE_ENGLISH
            href_uri = uri
        
        # code for alternate href tag
        SITE_URL_EN = environment_variables[env_var]['SITE_URL_EN'] + href_uri
        SITE_URL_ES = environment_variables[env_var]['SITE_URL_ES'] + href_uri
        
        UPPER = page_route.upper()
            
        TITLE_CONTENT = UPPER+"_TITLE"
        DESCRIPTION_CONTENT = UPPER+"_DESCRIPTION"
        
        # checking the title and description into property file 
        env_uri = config.get(message_by_language, TITLE_CONTENT, fallback = DEFAULT)
        
        # Setting title and description for default case
        if env_uri == DEFAULT:
            TITLE_CONTENT = DEFAULT_TITLE
            DESCRIPTION_CONTENT = DEFAULT_DESCRIPTION
        # Setting title and description for non OG Tag case
        title = config[message_by_language][TITLE_CONTENT]
        description = config[message_by_language][DESCRIPTION_CONTENT]
        
        # canonical URL changes for user home with and without user id 
        if uri.find(config['OG_URI']['HOME']) != -1 or uri.find(config['OG_URI']['HOME_URI']) != -1:
                if language_code == 165:
                    CANONICAL_URL_VALUE =  environment_variables[env_var]['SITE_URL'] + '/'
                elif language_code == 245:
                    CANONICAL_URL_VALUE =  environment_variables[env_var]['SITE_URL_SPANISH']
        
        # find OG Tag URI
        if uri.find(config['OG_URI']['HOME']) != -1 or uri.find(config['OG_URI']['PROFILE']) != -1:
            
            # calling get_details_for_og_tags for setting title and description for non OG Tag case
            title, description, USER_IMAGE =  get_details_for_og_tags(uri,page_route,language_code, message_by_language)
            
            if USER_IMAGE != "":
                IMAGE_URL = USER_IMAGE
             
        CONTENT = config[HTML_CONTENT][HTML_CONTENT_WITH_OG_TAG]
        
        logger.info(CONTENT)
            
        logger.info("language_code ::::::::::\n" + str(language_code))
        logger.info("locale ::::::::::\n" + locale)
        
        CONTENT = CONTENT.replace('SITE_URL_VALUE', SITE_URL).replace('USER_LOCALE',locale).replace('PAGE_TITLE',title).replace('PAGE_DESCRIPTION',description).replace('FACEBOOK_APP_ID',APP_ID).replace('USER_IMAGE_URL',IMAGE_URL).replace('SITE_URL_EN',SITE_URL_EN).replace('SITE_URL_ES',SITE_URL_ES).replace('CANONICAL_URL_VALUE',CANONICAL_URL_VALUE)
        response =  {
                        'status': '200',
                        'statusDescription': 'OK',
                        'headers': {
                            'cache-control': [
                                {
                                    'key': 'Cache-Control',
                                    'value': 'max-age=100'
                                }
                            ],
                            "content-type": [
                                {
                                    'key': 'Content-Type',
                                    'value': 'text/html'
                                }
                            ]
                        },
                        'body': CONTENT
                    }
        logger.info("response :::::::::\n")
        logger.info(response)
        return response
    except:
        # if above code fails than returning the json
       logger.error(traceback.format_exc())
       return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)


if __name__ == "__main__":
    handler(None,None)