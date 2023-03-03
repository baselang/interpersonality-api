"""API Module to provide Mystery details for user.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. success_json(): Function to create json response for success message
3. log_err(): Logging error and returning the JSON response with error message & status code
4. jwt_verify(): verifying token and fetching data from the jwt token sent by user
5. handler(): Handling the incoming request with following steps:
- Fetching the data
- fetching users mystery information
- Returning the JSON response with the required data and success status code

"""


import jwt
import json
import pymysql
import boto3
import logging
import traceback
from os import environ
from datetime import datetime  
from datetime import timedelta
from datetime import timezone
import configparser

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('mystery.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')
mystery_unlock_user_count = environ.get('MYSTERY_UNLOCK_USER_COUNT')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')


# aws cridentials required for creating boto3 client object
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')
ILLUSTRATION_BUCKET_URL = environ.get('ILLUSTRATION_BUCKET_URL')
test_token = environ.get('LOGIN_TOKEN')

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
# Setting the log level to INFO
logger.setLevel(logging_Level)

logger.info("Cold start complete.") 

def make_connection():
    """Function to make the database connection."""
    return pymysql.connect(host=endpoint, user=dbuser, passwd=password,
        port=int(port), db=database, autocommit=True)

def success_json(success_data):
    """Function to create json response for success message"""
    return  {
                'statusCode': 200,
                'headers':{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                          },
                'body': json.dumps(success_data)
            }

def jwt_verify(auth_token):
    """Function to verify the authorization token"""
    # decoding the authorization token provided by user
    payload = jwt.decode(auth_token, SECRET_KEY, options={'require_exp': True})
    
    # setting the required values in return
    rid = int(payload['id'])
    user_id = payload['user_id']
    language_id = payload['language_id']
    return rid, user_id, language_id

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

def filtering_user_report(user_profile_json, user_id, language_id, self_user, gender_id, section_id, style_code_for_section_overview, cursor):
    theme_14_section_list = []

    if (gender_id == 1):
        gender_id_value = 1
    else:
        gender_id_value = 0
    
    
    json_result_list = []

    data = user_profile_json
    
    lengthValues = len(data)

    for i in range(lengthValues):
        rows = str(data[i]['Rows'])
        number = str(data[i]['Number'])
        section = str(data[i]['Section'])
        wordCount = str(data[i]['Word_count'])
        illustration = str(data[i]['Illustration'])
        title = str(data[i]['Title'])
        story = str(data[i]['Story'])
        illustration_image = str(data[i]['illustration_image'])
        style_id = number[-2:]
        theme_id = number.replace(style_id, "")
        
        try:
            # Executing the Query
            if (section_id == -1):

                # Query for getting content data
                selectionQuery = "SELECT `content` FROM `profile_content` WHERE `theme_id`=%s AND `style_id`=%s AND `language_id`=%s AND `self_user`=%s AND `gender_id`=%s AND `section_id`=%s"
                cursor.execute(selectionQuery,
                               (theme_id, style_id, language_id, self_user, gender_id_value, section))
            else:
                
                # Query for getting content data
                selectionQuery = "SELECT `content` FROM `profile_content` WHERE `theme_id`=%s AND `style_id`=%s AND `language_id`=%s AND `self_user`=%s AND `gender_id`=%s AND `section_id`=%s"
                cursor.execute(selectionQuery,
                               (theme_id, style_id, language_id, self_user, gender_id_value, section_id))

            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            # getting content data
                
            if (result_list != []):
                content = result_list[0][0]
                json_result = {"Rows": rows, "Number": number, "Section": section, "Word_count": wordCount,
                               "Illustration": illustration, "Title": title, "Story": story,
                               "Content": content}
                if str(illustration) == "1" and illustration_image != "None" and illustration_image != "" and illustration_image != "nan":
                    json_result["illustration_image"] = ILLUSTRATION_BUCKET_URL + illustration_image
                json_result_list.append(json_result)
            else:
                json_result = {"Rows": rows, "Number": number, "Section": section, "Word_count": wordCount,
                               "Illustration": illustration, "Title": title, "Story": story,
                               "Content": "BLANK"}
                if str(illustration) == "1" and illustration_image != "None" and illustration_image != "" and illustration_image != "nan":
                    json_result["illustration_image"] = ILLUSTRATION_BUCKET_URL + illustration_image
                json_result_list.append(json_result)

        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
    
    return json_result_list
    
def get_extended_profile_data(cursor, rid, user_id, language_id, gender):
    """Function for getting extended profile related data"""
    
    try:
        invokeLam = make_client()
    except:
    	# If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['INVOCATION_ERROR'])
        
    # Fetching count from user_theme_style table
    selectionQuery = "SELECT `Rows`, `theme_style` AS `Number`, `theme_id` AS `Theme`, `style_id` AS `Style`, `section_id` AS `Section`, `word_count`, `illustration_flag` AS `Illustration`, `title` AS `Title`, `illustration_image`, `story`, `main_report` FROM `user_profile_report` WHERE `user_id`=%s ORDER BY `id`"
    # Executing the query
    cursor.execute(selectionQuery, (user_id))
    # executing the cursor
    ans_df_list = []
    
    self_user = 1
    section_id = -1

    # Fetching the result from the cursor
    for result in cursor: ans_df_list.append({'Rows':result[0], 'Number':result[1], 'Theme':result[2], 'Style':result[3], 'Section':result[4], 'Word_count':result[5], 'Illustration':result[6], 'Title':result[7], 'illustration_image':result[8], 'Story':result[9], 'main_report':result[10]})
    
    # Fetching the different types of data from the result_list
    extended_profile_data_df = [i for i in ans_df_list if i['main_report'] == 0]
    
    if len(ans_df_list)==0:
        try :
            # creating the custom payload
            payload = {"headers":{"user_id":user_id, "language_id":language_id, "lambda_source":"invoked_lambda", "report_tab":"INDIVIDUAL_FILTERED_V1", "self_user":self_user, "section_id":section_id, "test_token":test_token}}
            # invoking the lambda function with custom payload to take updated facebook image and update it for the user
            response = invokeLam.invoke(FunctionName= "ProfilesGenerateUserProfileReport" + ENVIRONMENT_TYPE, InvocationType="RequestResponse", Payload=json.dumps(payload))
            
            response = response['Payload']
            response = json.loads(response.read().decode("utf-8"))
            
            # getting language_id from response
            extended_profile_data_df = json.loads(response['body'])['extended_report_content']
            
        except:
            # sending error when user_id or user is invalid
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['INVOCATION_ERROR'], 500)
    
    # Fetching count from user_theme_style table
    selectionQuery = "SELECT `theme_id` as `theme`, `style_id` as `style` FROM `user_theme_style` WHERE `rid`=%s AND `theme_id`=14"
    # executing the cursor
    cursor.execute(selectionQuery, (rid))
    
    user_theme_style_list = []
    
    # fetching the result from the cursor
    for result in cursor: user_theme_style_list.append(result)
    
    try:
        theme_value = user_theme_style_list[0][0]
        style_value  = user_theme_style_list[0][1]
        value1 = str(int(theme_value))
        value2 = str(int(style_value))
        style_code_for_section_overview = value1+"-"+value2
    except:
        logger.error(traceback.format_exc())
    
    # filtering the user report to get all the text to show
    json_result_list = filtering_user_report(extended_profile_data_df, user_id, language_id, self_user, gender, section_id, style_code_for_section_overview, cursor)

    return json_result_list

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
    """Function to handle the request for Get Big5 API."""
    logger.info(event)
    global message_by_language
    global mystery_unlock_user_count
    try:
        auth_token = event['headers']['Authorization']
        user_id = event['headers']['user_id']
        rid = event['headers']['rid']
        mystery_unlock_user_count = int(mystery_unlock_user_count)
    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
        
    try:
        # verifying that the user is authorized or not to see this api's data
        rid, user_id, language_id = jwt_verify(auth_token)
    except:
        # if user does not have valid authorization
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['UNAUTHORIZED'], 403)
        
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        
        try:
            # Query for getting current language of the user
            selectionQuery = "SELECT `language_id` FROM `users` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (rid))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            # getting current language_id of the user 
            language_id = result_list[0][0]
            message_by_language = str(language_id) + "_MESSAGES"
        except:
            # If there is any error in above operations, logging the error
            return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
        
        try:
            # selection query to update mystery start time and mystery status
            updationQuery = "UPDATE `users` SET `mystery_start_time`= CASE WHEN `mystery_status` = 0 AND `mystery_friend_join_counter` < %s THEN NOW() ELSE `mystery_start_time` END,`mystery_status`= CASE WHEN `mystery_status`=1 AND NOW()>DATE_ADD(`mystery_start_time`, INTERVAL 24 HOUR) AND `mystery_friend_join_counter` < %s THEN 3 WHEN `mystery_status`=0 AND `mystery_friend_join_counter` < %s THEN 1 ELSE `mystery_status` END WHERE `id`=%s"
            
            # Executing the updation query
            cursor.execute(updationQuery, (mystery_unlock_user_count, mystery_unlock_user_count, mystery_unlock_user_count, rid))
        except:
            logger.error(traceback.format_exc())
            log_err(config[message_by_language]['UPDATION_STATUS'], 500)
        
        try:
            # selection query to get mystery data from user
            selectionQuery = "SELECT `id`, `mystery_status`, TIMEDIFF(DATE_ADD(`mystery_start_time`, INTERVAL 24 HOUR),NOW()), `mystery_friend_join_counter`, `is_mystery_visited`, `gender` FROM `users` WHERE `id`=%s"
            cursor.execute(selectionQuery, (rid))
            result_list = []
            for result in cursor: result_list.append(result)
            rid = result_list[0][0]
            mystery_status = result_list[0][1]
            mystery_start_time = result_list[0][2]
            
            if mystery_start_time != None:
                # if mystery_start_time is not none then finding seconds
                mystery_start_time = mystery_start_time.seconds
                
                if int(mystery_start_time)==0:
                    mystery_start_time = 86400
                    
            mystery_friend_join_counter = result_list[0][3]
            is_mystery_visited = result_list[0][4]
            gender = int(result_list[0][5])
            if mystery_status == 1:
                
                # Fetching the no of rows in each section for extended profile
                selectionQuery = "SELECT `section_id`, COUNT(*) FROM `user_profile_report` WHERE `rid`=%s AND `main_report` = 0 GROUP BY `section_id`"
                # Executing the Query
                cursor.execute(selectionQuery, (rid))
                
                section_count_list = []
                
                # Fetching the result from the cursor
                for result in cursor: section_count_list.append({"Section" : int(result[0]), "count" : int(result[1])})

                # when mystery timer is started
                return  success_json({'mystery_status':mystery_status, 'mystery_friend_join_counter':mystery_friend_join_counter, 'mystery_start_time':mystery_start_time, "sections_count":section_count_list})

                
            elif mystery_status == 2:
                # when mystery is unlocked
                if int(is_mystery_visited) == 0:
                    # updation query for mystery is visited after unlock or not visited
                    updationQuery = "UPDATE `users` SET `is_mystery_visited`=1 WHERE `id`=%s"
                    # Executing the query
                    cursor.execute(updationQuery, (rid))

                # getting the data for extended profile
                extended_report_content = get_extended_profile_data(cursor, rid, user_id, language_id, gender)

                # returning the success json with required data
                return  success_json({'mystery_status':mystery_status, 'message':config[message_by_language]['MYSTERY_STATUS_SUCCESSFULLY_UNLOCKED'], 'extended_report_content':extended_report_content})
                
            elif mystery_status == 3:
                # when user failed to unlock mystery
                return  success_json({'mystery_status':mystery_status, 'message': config[message_by_language]['MYSTERY_STATUS_UNSUCCESSFULLY_UNLOCKED']})
                
            else:
                # when mystery timer is not started
                return  success_json({'mystery_status':mystery_status, 'message': config[message_by_language]['MYSTERY_STATUS']})
                
        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
            
    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)
    finally:
        try:
            # Finally, clean up the connection
            cnx.close()
        except: 
            pass 
    

if __name__== "__main__":
    handler(None,None)