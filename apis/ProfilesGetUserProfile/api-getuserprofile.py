"""API Module to take out user profile details.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. others_profile(): Function to fetch users details who is logged in or logged out and accessing others profile
4. make_client(): Function Making a boto3 aws client to perform invoking of functions
5. is_url_image(): Function validate the format of image
6. same_profile(): Function to fetch users details who is logged in and accessing his own profile
7. jwt_verify(): Function to verify the authorization token
8.filtering_user_report(): Function filtering the user report
9. handler(): Handling the incoming request with following steps:
- Getting required data from user
- getting profile data of user
- Returning the JSON response with the requested data and success status code

"""
import jwt
import json
import pymysql
import logging
import traceback
from os import environ
import configparser
import boto3
from urllib.request import urlopen

message_by_language = "165_MESSAGES"
language_id = 165

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('getuserprofile.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port = environ.get('PORT')
dbuser = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')
key = environ.get('DB_ENCRYPTION_KEY')
url = environ.get('GET_LANGUAGE')

# aws cridentials required for creating boto3 client object
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')

# Getting key for verification of token
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')
ILLUSTRATION_BUCKET_URL = environ.get('ILLUSTRATION_BUCKET_URL')
REPLACED_SUB_STRING = environ.get('REPLACED_SUB_STRING')

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
    return {
        "statusCode": status_code,
        "body": json.dumps({"message": errmsg}),
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': 'true'},
        "isBase64Encoded": "false"
    }


def jwt_verify(auth_token):
    """Function to verify the authorization token"""
    payload = jwt.decode(auth_token, SECRET_KEY, options={'require_exp': True})
    return payload


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

def is_url_image(url):
    image_formats = ("image/png", "image/jpeg", "image/gif", "image/jpg")
    try:
        site = urlopen(url)
        meta = site.info()  # get header of the http request
        logger.info(meta)
        if meta["content-type"] in image_formats:  # check if the content-type is a image
            return "False"
    except:
        return "True"

def get_user_share_cta(cursor, user_profile_df, language_id):
    """Getting the share cta report for the user"""
    
    contentQuery = "SELECT `section_id`,`theme_id`, `style_id`, `content` FROM `call_to_action_content` WHERE `language_id` = %s"
    cursor.execute(contentQuery, (language_id))
    content_list = cursor.fetchall()
    
    result_list = []
    if len(content_list) != 0:
        
        for x in range(len(content_list)):
            data = []
            section_of_content = content_list[x][0]
            theme_id = content_list[x][1]
            style_id = content_list[x][2]
            call_to_action = str(content_list[x][3])
            data.append(section_of_content)
            data.append(theme_id)
            data.append(style_id)
            data.append(call_to_action)
            result_list.append(data)
    else:
        contentQuery = "SELECT `section_id`,`theme_id`, `style_id` FROM `call_to_action_content`"
        cursor.execute(contentQuery)
        content_list = cursor.fetchall()
        
        for x in range(len(content_list)):
            data = []
            section_of_content = content_list[x][0]
            theme_id = content_list[x][1]
            style_id = content_list[x][2]
            call_to_action = "BLANK"
            data.append(section_of_content)
            data.append(theme_id)
            data.append(style_id)
            data.append(call_to_action)
            result_list.append(data)
        
    share_module_user = [{"section_id":i[0], 'theme_id':i[1], 'style_id':i[2], 'call_to_action':i[3]} for i in result_list]
    
    # Query for fetching section_id related to an user
    selectionQuery = "SELECT `section_id` FROM `section_detail` WHERE `section_id` NOT IN (0)"
    # Execution of query
    cursor.execute(selectionQuery)
    
    section_num = []
    # Fetching the sections from the cursor
    for result in cursor: section_num.append(result[0])
    
    # Query for fetching section_id related to an user
    selectionQuery = "SELECT DISTINCT(`theme_id`) FROM `theme_names`"
    # Execution of query
    cursor.execute(selectionQuery)
    
    theme_num = []
    # Fetching the sections from the cursor
    for result in cursor: theme_num.append(result[0])
    
    # Read Share Modules csv file.
    share_module_report_df = []
    
    users_sections_list = [int(i['Section']) for i in user_profile_df]
    
    for section_number in section_num:
        module_report_dataframe = []
        for theme_number in theme_num:
            if module_report_dataframe == []:
                section_and_theme_wise_user_profile_df = [i['Style'] for i in user_profile_df if int(i['Section'])==int(section_number) and int(i['Theme']) == int(theme_number)]
                module_report_dataframe = [i for i in share_module_user if int(i['section_id']) == int(section_number) and i['theme_id'] == int(theme_number) and i['style_id'] in section_and_theme_wise_user_profile_df]
        if module_report_dataframe == []:
            module_report_dataframe = [i for i in share_module_user if int(i['theme_id'])==-1 and int(i['section_id']) in users_sections_list and int(i['style_id'])==-1 and int(i['section_id'])==int(section_number)]
            
        share_module_report_df.extend(module_report_dataframe)
        
    users_report_data_4 = [
                {"Rows": "", "Number": "", "Section": i['section_id'], "Word_count": "", "Illustration": "", "Title": "",
                 "Story": "", "Content": i['call_to_action']} for i in share_module_report_df]

    return users_report_data_4

def filtering_user_report(user_profile_json, user_id, language_id, self_user, gender_id, section_id, style_code_for_section_overview, cursor, firstname):

    theme_14_section_list = []

    if (gender_id == 1):
        gender_id_value = 1
    else:
        gender_id_value = 0

    json_result_list = []
    number_list = []
    section_list = []

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

        if section not in section_list:
            number_list = []
            section_list.append(section)

        try:
            # Executing the Query
            if (section_id == -1):
                try:
                    if section not in theme_14_section_list:
                        theme_14_section_list.append(section)
                        # Query for getting overview data
                        selectionQuery = "SELECT `overview` FROM `section_overview` WHERE `theme_id`=%s AND `style_id`=%s  AND `language_id`=%s AND `gender_id`=%s AND `self_user`=%s  AND `section_id`=%s"

                        themeId = ''
                        styleId = ''

                        themeId = style_code_for_section_overview.split('-')[0]
                        styleId = style_code_for_section_overview.split('-')[1]
                        styleId = styleId.zfill(2)

                        cursor.execute(selectionQuery, (themeId, styleId, language_id, gender_id_value, self_user, section))
                        result_list = []
                        # fetching result from the cursor
                        for result in cursor: result_list.append(result)

                        if result_list:
                            overview = result_list[0][0]
                            if self_user != 1:
                                overview = overview.replace(REPLACED_SUB_STRING, firstname)

                            if (result_list != []):
                                json_result = {"Rows": "", "Number": themeId + styleId, "Section": section,
                                               "Word_count": "", "Illustration": "", "Title": "", "Story": "0",
                                               "Content": overview}
                                if str(json_result['Number']) not in number_list:
                                    json_result_list.append(json_result)
                                    number_list.append(str(json_result['Number']))
                except:
                    logger.error(traceback.format_exc())

                # Query for getting content data
                selectionQuery = "SELECT `content` FROM `profile_content` WHERE `theme_id`=%s AND `style_id`=%s AND `language_id`=%s AND `self_user`=%s AND `gender_id`=%s AND `section_id`=%s"
                cursor.execute(selectionQuery, (theme_id, style_id, language_id, self_user, gender_id_value, section))
            else:
                try:
                    if section_id not in theme_14_section_list:
                        theme_14_section_list.append(section_id)
                        # Query for getting overview data
                        selectionQuery = "SELECT `overview` FROM `section_overview` WHERE `theme_id`=%s AND `style_id`=%s AND `language_id`=%s AND `gender_id`=%s AND `self_user`=%s AND `section_id`=%s"

                        themeId = ''
                        styleId = ''

                        themeId = style_code_for_section_overview.split('-')[0]
                        styleId = style_code_for_section_overview.split('-')[1]
                        styleId = styleId.zfill(2)

                        cursor.execute(selectionQuery, (themeId, styleId, language_id, gender_id_value, self_user, section_id))
                        result_list = []
                        # fetching result from the cursor
                        for result in cursor: result_list.append(result)

                        if result_list:
                            overview = result_list[0][0]
                            if self_user != 1:
                                overview = overview.replace(REPLACED_SUB_STRING, firstname)

                            if (result_list != []):
                                json_result = {"Rows": "", "Number": themeId + styleId, "Section": section_id,
                                               "Word_count": "", "Illustration": "", "Title": "", "Story": "0",
                                               "Content": overview}
                                if str(json_result['Number']) not in number_list:
                                    json_result_list.append(json_result)
                                    number_list.append(str(json_result['Number']))
                except:
                    logger.error(traceback.format_exc())
                # Query for getting content data
                selectionQuery = "SELECT `content` FROM `profile_content` WHERE `theme_id`=%s AND `style_id`=%s AND `language_id`=%s AND `self_user`=%s AND `gender_id`=%s AND `section_id`=%s"
                cursor.execute(selectionQuery, (theme_id, style_id, language_id, self_user, gender_id_value, section_id))

            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)

            # getting content data
            if (result_list != []):
                content = result_list[0][0]
                if self_user != 1:
                    content = content.replace(REPLACED_SUB_STRING, firstname)

                json_result = {"Rows": rows, "Number": number, "Section": section, "Word_count": wordCount,
                               "Illustration": illustration, "Title": title, "Story": story,
                               "Content": content}
                if str(illustration) == "1" and illustration_image != "None" and illustration_image != "" and illustration_image != "nan":
                    json_result["illustration_image"] = ILLUSTRATION_BUCKET_URL + illustration_image
                if str(json_result['Number']) not in number_list:
                    json_result_list.append(json_result)
                    number_list.append(str(json_result['Number']))
            else:
                json_result = {"Rows": rows, "Number": number, "Section": section, "Word_count": wordCount,
                               "Illustration": illustration, "Title": title, "Story": story,
                               "Content": "BLANK"}
                if str(illustration) == "1" and illustration_image != "None" and illustration_image != "" and illustration_image != "nan":
                    json_result["illustration_image"] = ILLUSTRATION_BUCKET_URL + illustration_image
                if str(json_result['Number']) not in number_list:
                    json_result_list.append(json_result)
                    number_list.append(str(json_result['Number']))

        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())

    return json_result_list

def others_profile(cursor, user_id, invokeLam, language_id, rid):
    """Used when is user is logged in or logged out and accessing others profile"""
    
    try:
        self_user = 0
        section_id = -1
        
        # making query to fetch data required for profile
        query = "SELECT cast(AES_DECRYPT(u.`firstname`,%s) as char),u.`id`,u.`picture_url`,u.`gender`,u.`scrape_image_url`,(CASE WHEN NOW() > DATE_ADD(u.`fb_data_updation_time`, INTERVAL 30 DAY) AND u.`is_picture_uploaded`=0 AND u.`social_userid` IS NOT NULL AND u.`picture_url` IS NOT NULL THEN 1 ELSE 0 END), u.`social_userid`, u.`is_picture_uploaded` FROM `users` u WHERE u.`id`=%s"
        
        # Executing query using cursor
        cursor.execute(query, (key, rid))
        
        try:
            # fetching results from the executed query
            result_list = []
            for result in cursor: result_list.append(result)
            rid = result_list[0][1]
            firstname = result_list[0][0]
            picture_url = result_list[0][2]
            gender = int(result_list[0][3])
            scrape_image_url = result_list[0][4]
            is_update_image = int(result_list[0][5])
            social_userid = result_list[0][6]
            is_picture_uploaded = result_list[0][7]
            
            if social_userid != None and is_picture_uploaded !=1:
                if is_url_image(picture_url) == "True":
                    picture_url = None
                    try:
                        # making an boto 3 client object
                        invokeLam = make_client()

                        # preparing the payload for lambda invocation
                        payload = {"headers":{"user_id": user_id}}

                        # invoking the lambda function with custom payload
                        response = invokeLam.invoke(FunctionName= "ProfilesUserRegenerateFacebookImages" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))

                    except:
                        # If there is any error in above operations, logging the error
                        logger.error(traceback.format_exc())
                        return log_err(config[message_by_language]['INVOCATION_ERROR'])
                        
        except:
            # sending error when user_id or user is invalid
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['USER_STATUS'], 500)

        if is_update_image == 1:
            try:
                # creating the custom payload
                payload = {"rid": rid}
                # invoking the lambda function with custom payload to take updated facebook image and update it for the user
                invokeLam.invoke(FunctionName="ProfilesUpdateFacebookDetailsAsync" + ENVIRONMENT_TYPE,
                                 InvocationType="Event", Payload=json.dumps(payload))
            except:
                # sending error when user_id or user is invalid
                logger.error(traceback.format_exc())
                return log_err(config[message_by_language]['INVOCATION_ERROR'], 500)
                
        # Fetching count from user_theme_style table
        selectionQuery = "SELECT `Rows`, `theme_style` AS `Number`, `theme_id` AS `Theme`, `style_id` AS `Style`, `section_id` AS `Section`, `word_count`, `illustration_flag` AS `Illustration`, `title` AS `Title`, `illustration_image`, `story`, `main_report` FROM `user_profile_report` WHERE `user_id`=%s ORDER BY `id`"
        # Executing the query
        cursor.execute(selectionQuery, (user_id))
        # executing the cursor
        ans_df_list = []
        
        # Fetching the result from the cursor
        for result in cursor: ans_df_list.append({'Rows':result[0], 'Number':result[1], 'Theme':result[2], 'Style':result[3], 'Section':result[4], 'Word_count':result[5], 'Illustration':result[6], 'Title':result[7], 'illustration_image':result[8], 'Story':result[9], 'main_report':result[10]})
        
        # Fetching the different types of data from the result_list
        user_profile_df = [i for i in ans_df_list if i['main_report'] == 1 and i['Section'] != 0]
        user_summaries_report = [i for i in ans_df_list if i['main_report'] == 1 and i['Section'] == 0]
        
        
        if len(ans_df_list)==0:
            try :
                
                # creating the custom payload
                payload = {"headers":{"user_id":user_id, "language_id":language_id, "lambda_source":"invoked_lambda", "report_tab":"INDIVIDUAL_FILTERED_V1", "self_user":self_user, "section_id":section_id}}
                # invoking the lambda function with custom payload to take updated facebook image and update it for the user
                response = invokeLam.invoke(FunctionName= "ProfilesGenerateUserProfileReport" + ENVIRONMENT_TYPE, InvocationType="RequestResponse", Payload=json.dumps(payload))
                
                response = response['Payload']
                response = json.loads(response.read().decode("utf-8"))
                
                # getting language_id from response
                user_profile_df = json.loads(response['body'])['user_profile_report']
                user_summaries_report = json.loads(response['body'])['profiles_summaries_report']
                
            except:
                # sending error when user_id or user is invalid
                logger.error(traceback.format_exc())
                return log_err(config[message_by_language]['INVOCATION_ERROR'], 500)
                
        user_summaries_report.extend(user_profile_df)
        user_profile_df = user_summaries_report
        
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
            logger.info("style_code_for_section_overview :"+style_code_for_section_overview)
        except:
            logger.error(traceback.format_exc())
        
        # filtering the user report to get all the text to show
        json_result_list = filtering_user_report(user_profile_df, user_id, language_id, self_user, gender, section_id, style_code_for_section_overview, cursor, firstname)
        
    except:
        # when there is some problem in executing query
        logger.info(traceback.format_exc())
        return log_err(config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
    
    # returning success json to be send in response
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': 'true'
        },
        'body': {'user_id': user_id, 'firstname': firstname, 'picture_url': picture_url, 'gender': gender, 'scrape_image_url': scrape_image_url, "users_report_data" : json_result_list}
    }


def same_profile(cursor, user_id, auth_token, invokeLam, rid):
    """Used when is user is logged in and accessing his own profile"""
    
    try:
        self_user = 1
        section_id = -1
        
        # making query to fetch data required for profile
        query = "SELECT cast(AES_DECRYPT(u.`firstname`,%s) as char),u.`id`,u.`picture_url`,u.`language_id`,u.`gender`,u.`scrape_image_url`,(CASE WHEN NOW() > DATE_ADD(u.`fb_data_updation_time`, INTERVAL 30 DAY) AND u.`is_picture_uploaded`=0 AND u.`social_userid` IS NOT NULL AND u.`picture_url` IS NOT NULL THEN 1 ELSE 0 END), u.`social_userid`, u.`is_picture_uploaded` FROM `users` u WHERE u.`id`=%s"
        
        # Executing query using cursor
        cursor.execute(query, (key, rid))

        try:
            # fetching results from the executed query
            result_list = []
            # fetching result from cursor
            for result in cursor: result_list.append(result)
            rid = result_list[0][1]
            firstname = result_list[0][0]
            language_id = result_list[0][3]
            picture_url = result_list[0][2]
            gender = int(result_list[0][4])
            scrape_image_url = result_list[0][5]
            is_update_image = result_list[0][6]
            social_userid = result_list[0][7]
            is_picture_uploaded = result_list[0][8]
            
            if social_userid != None and is_picture_uploaded != 1:
                if is_url_image(picture_url) == "True":
                    picture_url = None
                    try:
                        # making an boto 3 client object
                        invokeLam = make_client()

                        # preparing the payload for lambda invocation
                        payload = {"headers": {"user_id": user_id}}

                        # invoking the lambda function with custom payload
                        response = invokeLam.invoke(FunctionName="ProfilesUserRegenerateFacebookImages" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))

                    except:
                        # If there is any error in above operations, logging the error
                        logger.error(traceback.format_exc())
                        return log_err(config[message_by_language]['INVOCATION_ERROR'])

            # Commented the below code due to the removal of Facebook Friends Functionality
            # try:
            #     # making an boto 3 client object
            #     invokeLam = make_client()
            #
            #     # preparing the payload for lambda invocation
            #     payload = {"headers": {"Authorization": auth_token}}
            #
            #     # invoking the lambda function with custom payload
            #     response = invokeLam.invoke(FunctionName="ProfilesRegenerateFacebookImages" + ENVIRONMENT_TYPE,
            #                                 InvocationType="Event", Payload=json.dumps(payload))
            #
            # except:
            #     # If there is any error in above operations, logging the error
            #     logger.error(traceback.format_exc())
            #     return log_err(config[message_by_language]['INVOCATION_ERROR'])

        except:
            # sending error when user_id or user is invalid
            return log_err(config[message_by_language]['USER_STATUS'], 500)

        if is_update_image == 1:
            try:
                # creating the custom payload
                payload = {"rid": rid}
                # invoking the lambda function with custom payload to take updated facebook image and update it for the user
                invokeLam.invoke(FunctionName="ProfilesUpdateFacebookDetailsAsync" + ENVIRONMENT_TYPE,
                                 InvocationType="Event", Payload=json.dumps(payload))
            except:
                # sending error when user_id or user is invalid
                return log_err(config[message_by_language]['INVOCATION_ERROR'], 500)

        # Fetching count from user_theme_style table
        selectionQuery = "SELECT `Rows`, `theme_style` AS `Number`, `theme_id` AS `Theme`, `style_id` AS `Style`, `section_id` AS `Section`, `word_count`, `illustration_flag` AS `Illustration`, `title` AS `Title`, `illustration_image`, `story`, `main_report` FROM `user_profile_report` WHERE `user_id`=%s ORDER BY `id`"
        # Executing the query
        cursor.execute(selectionQuery, (user_id))
        # executing the cursor
        ans_df_list = []
        
        # Fetching the result from the cursor
        for result in cursor: ans_df_list.append({'Rows':result[0], 'Number':result[1], 'Theme':result[2], 'Style':result[3], 'Section':result[4], 'Word_count':result[5], 'Illustration':result[6], 'Title':result[7], 'illustration_image':result[8], 'Story':result[9], 'main_report':result[10]})
        
        # Fetching the different types of data from the result_list
        user_profile_df = [i for i in ans_df_list if i['main_report'] == 1 and i['Section'] != 0]
        
        if len(ans_df_list)==0:
            try :
                # creating the custom payload
                payload = {"headers":{"user_id":user_id, "language_id":language_id, "lambda_source":"invoked_lambda", "report_tab":"INDIVIDUAL_FILTERED_V1", "self_user":self_user, "section_id":section_id}}
                # invoking the lambda function with custom payload to take updated facebook image and update it for the user
                response = invokeLam.invoke(FunctionName= "ProfilesGenerateUserProfileReport" + ENVIRONMENT_TYPE, InvocationType="RequestResponse", Payload=json.dumps(payload))
                
                response = response['Payload']
                response = json.loads(response.read().decode("utf-8"))
                logger.info(response)
                # getting language_id from response
                user_profile_df = json.loads(response['body'])['user_profile_report']
                
            except:
                # sending error when user_id or user is invalid
                logger.error(traceback.format_exc())
                return log_err(config[message_by_language]['INVOCATION_ERROR'], 500)
        
        # Fetching the share cta data for the user
        users_report_data_4 = get_user_share_cta(cursor, user_profile_df, language_id)
        
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
            logger.info("style_code_for_section_overview :"+style_code_for_section_overview)
        except:
            logger.error(traceback.format_exc())
        
        # filtering the user report to get all the text to show
        json_result_list = filtering_user_report(user_profile_df, user_id, language_id, self_user, gender, section_id, style_code_for_section_overview, cursor, firstname)
        
    except:
        # when there is some problem in executing query
        logger.info(traceback.format_exc())
        return log_err(config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
        
    # returning success json to be send in response
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': 'true'
        },
        'body': {'user_id': user_id, 'firstname': firstname, 'picture_url': picture_url, 'language_id': language_id, 'gender': gender,
                 'scrape_image_url': scrape_image_url, "users_report_data" : json_result_list, "share_module_report_content":users_report_data_4}
    }


def handler(event, context):
    """Function to handle the request for Get User Details api"""
    global message_by_language
    global url
    global language_id
    
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
        # making an boto 3 client object
        invokeLam = make_client()
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['INVOCATION_ERROR'])

    try:
        # getting data out of event
        language_id = int(event['headers']['language_id'])
        message_by_language = str(language_id) + "_MESSAGES"
    except:
        try:
            # invoking the lambda function with custom payload
            response = invokeLam.invoke(FunctionName="ProfilesGetLanguage" + ENVIRONMENT_TYPE,
                                        InvocationType="RequestResponse", Payload=json.dumps(
                    {"headers": {"Accept-Language": event['headers']['Accept-Language']}}))
            response = response['Payload']
            response = json.loads(response.read().decode("utf-8"))

            # getting language_id from response
            language_id = json.loads(response['body'])['language_id']
            message_by_language = str(language_id) + "_MESSAGES"
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['INVOCATION_ERROR'], 500)

    try:
        user_id = event['headers']['user_id']
        try:
            rid = event['headers']['rid']
        except:
            rid = 0
        try:
            # getting access_token from event or request object
            auth_token = event['headers']['Authorization']
            logger.info(user_id)
        except:
            auth_token = ""
    except:
        # if data is not sent or sent in wrong format
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
    try:
        # Making the DB connection
        cnx = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
    except:
        # when can't be made a connection
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)

    try:
        user_result_list = []
        # query for user details
        selectionQuery = "SELECT cast(AES_DECRYPT(`firstname`, %s) as char), `language_id`, `is_active`, `id` FROM `users` WHERE `user_id` = %s"
        # Executing the query
        cursor.execute(selectionQuery, (key, user_id))
        # Fetching the result of above query from cursor
        for result in cursor: user_result_list.append(result)
        
        try:
            # checking that user with provided user_id exist or not
            rid = int(user_result_list[0][3])
        except:
            # sending error when user_id or user is invalid
            return log_err(config[message_by_language]['USER_STATUS'], 404)
        
        # checking that user has given complete test or not
        if int(user_result_list[0][2]) != 1:
            if auth_token == "":
                # returning json if authentication token is not present or user is not logged in
                return {
                    'statusCode':404,
                    'headers': {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Credentials': 'true'
                    },
                    'body': json.dumps({
                        'test_status': 'not_completed',
                        'user_id': user_id,
                        'firstname': user_result_list[0][0],
                        'language_id': user_result_list[0][1]
                    })
                }
            else:
                # returning json if authentication token is present or user is logged in
                return {
                    'statusCode': 404,
                    'headers': {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Credentials': 'true'
                    },
                    'body': json.dumps({
                        'test_status': 'not_completed',
                        'auth': auth_token,
                        'user_id': user_id,
                        'firstname': user_result_list[0][0],
                        'language_id': user_result_list[0][1]
                    })
                }
    except:
        logger.info(traceback.format_exc())
        return log_err(config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
    try:
        # verifying access_token
        payload = jwt_verify(auth_token)
        
        if payload['user_id'] == user_id:
            # response when user is logged in and accessing own profile
            response = same_profile(cursor, user_id, auth_token, invokeLam, rid)

            if isinstance(response['body'], str):
                # if there is any error in fetching result
                return response

            # adding some elements to success json returned in response
            response['body']['auth'] = auth_token
            response['body']['access_type'] = 'is_login_private'
            response['body'] = json.dumps(response['body'])
            return response

        else:
            user_info = []
            # Query for getting user existence count
            selectionQuery = "SELECT `language_id` FROM `users` WHERE `user_id`=%s"
            # Executing the query
            cursor.execute(selectionQuery, (payload['user_id']))
            # Fetching result from cursor
            for result in cursor: user_info.append(result)
            # fetching language_id from cursor
            language_id = user_info[0][0]
            
            # response when user is logged in and accessing others profile
            response = others_profile(cursor, user_id, invokeLam, language_id, rid)

            if isinstance(response['body'], str):
                # if there is any error in fetching result
                return response

            # adding some elements to success json returned in response
            response['body']['language_id'] = language_id
            response['body']['auth'] = auth_token
            response['body']['access_type'] = 'is_login_public'
            response['body'] = json.dumps(response['body'])
            return response
    except:
        # response when user is offline or logged out and accessing others profile
        response = others_profile(cursor, user_id, invokeLam, language_id, rid)

        if isinstance(response['body'], str):
            # if there is any error in fetching result
            return response

        # adding some elements to success json returned in response
        response['body']['language_id'] = language_id
        response['body']['access_type'] = 'is_logged_out_public'
        response['body'] = json.dumps(response['body'])
        
        return response
    finally:
        try:
            # Finally, clean up the connection
            cursor.close
            cnx.close()
        except:
            pass


if __name__ == "__main__":
    handler(None, None)