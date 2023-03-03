"""API For generating the interpersonal report for user and his partner or friend for test interface and webapp

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. format_results(): function for formatting data to get the end result
5. handler(): Handling the incoming request with following steps:
- Fetching data from request
- generating the interpersonal report for the user
- Returning the JSON response with success status code with the required data
"""

import jwt
import json
import pymysql
import logging
import traceback
from os import environ
import configparser
import boto3
import pandas as pd
import time


# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('interpersonal_report.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

# variables required for the function
TO_BE_REPLACED = environ.get('TO_BE_REPLACED')

# variables for default similarity score
DEFAULT_SIMILARITY_SCORE = int(environ.get('DEFAULT_SIMILARITY_SCORE'))

# Login Token 
LOGIN_TOKEN = environ.get('LOGIN_TOKEN')

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# getting message variable
message_by_language = "165_MESSAGES"

# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
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
    return  {
                "statusCode": status_code,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }
        
def jwt_verify(auth_token):
    """Function to verify the authorization token"""
    # decoding the authorization token provided by user
    payload = jwt.decode(auth_token, SECRET_KEY, options={'require_exp': True})
    
    # setting the required values in return
    rid = int(payload['id'])
    user_id = payload['user_id']
    language_id = payload['language_id']
    return rid, user_id

def retrieve_users_report(user_ids, db_connection):
    """Retrieve the user's input data from the MySQL database"""
    
    # constructing the query for fetching data from the tables
    query = "SELECT input.`user_id`, input.`C1`, input.`C2`, input.`C3`, input.`C4`, input.`C5`, input.`C6`, input.`E1`, input.`E2`, input.`E3`, input.`E4`, input.`E5`, input.`E6`, input.`O1`, input.`O2`, input.`O3`, input.`O4`, input.`O5`, input.`O6`, input.`N1`, input.`N2`, input.`N3`, input.`N4`, input.`N5`, input.`N6`, input.`A1`, input.`A2`, input.`A3`, input.`A4`, input.`A5`, input.`A6`, (SELECT ug.`gender` FROM `user_gender` ug WHERE `id` = users.`gender`) as `gender`, users.`age` FROM `user_input_variables_30` input, `users` users WHERE users.`user_id`=input.`user_id` AND users.`user_id` IN (%(first_user)s, %(second_user)s) ORDER BY FIELD(users.`user_id`, %(order_first)s, %(order_second)s)"
    
    # Executing the query for fetching data from database
    users_report_df = pd.read_sql(query, db_connection, params={"first_user":user_ids[0], "second_user":user_ids[1], "order_first":user_ids[0], "order_second":user_ids[1]})
    
    # if the user are
    if user_ids[0] == user_ids[1]:
        users_report_df = pd.concat([users_report_df, users_report_df])
    
    # resetting the index of the dataframe
    users_report_df = users_report_df.reset_index(drop=True)
    
    # returning the user report dataframe
    return users_report_df

def calculate_similarity_score(users_report_df):
    """"Finding correlation between two dataframes"""
    try:
        
        # Removing unwanted column from the data frame
        users_report_df = users_report_df.drop(["age", "gender"], axis = 1)
        
        # making each dataframe as a dataframe to correlate it
        first_user_trait_scores_df = pd.DataFrame([users_report_df.iloc[0,1:]])
        second_user_trait_scores_df = pd.DataFrame([users_report_df.iloc[1,1:]])
        
        # reindexing both the dataframe or preprocessing it before correlating it
        first_user_trait_scores_df = first_user_trait_scores_df.reset_index(drop=True)
        second_user_trait_scores_df = second_user_trait_scores_df.reset_index(drop=True)
        
        # To find the correlation among the 
        # columns of df1 and df2 along the row axis 
        correlation_value = first_user_trait_scores_df.corrwith(second_user_trait_scores_df, axis=1)
        
        # calculating the similarity_score by using the correlation value
        similarity_score = int(round((((correlation_value[0] + 1)/2))*100))
    except:
        similarity_score = DEFAULT_SIMILARITY_SCORE
    
    return similarity_score

def format_results(pair_report_list, ordering_list, user_1_gender, user_2_gender, language_id, cursor, user2_name, db_connection):
    # Retrieve the difference scores for each user pair
    user_1_list = []
    user_2_list = []
    comparison = []
    user_1_content = []
    user_2_content = []
    style_difference_scores = []
    theme = []
    
    try:
        
        if(user_1_gender=='female'):
            user_1_gender_id = 1
        else:
            user_1_gender_id = 0
            
        if(user_2_gender=='female'):
            user_2_gender_id = 1
        else:
            user_2_gender_id = 0 
        
        selectionQuery = "SELECT `difference_score`, `style1`, `style2` FROM `style_difference_scores`"
        difference_scores = pd.read_sql(selectionQuery, db_connection)
        
        selectionQuery = "SELECT `name`, `theme_id` FROM `theme_names` WHERE `language_id` = %(lang_id)s"
        theme_names = pd.read_sql(selectionQuery, db_connection, params={"lang_id":language_id})
        
        selectionQuery = "SELECT `score_text`, `scores` FROM `scores_text` WHERE `language_id` = %(lang_id)s"
        scores_text = pd.read_sql(selectionQuery, db_connection, params={"lang_id":language_id})
        
        selectionQuery = "SELECT `content`, `theme_id`, `style_id` FROM `free_interpersonal_content` where `language_id` = %(lang_id)s AND `gender_id`=%(user_1_gender)s AND `self_user`=%(user_self)s"
        interpersonal_content_1 = pd.read_sql(selectionQuery, db_connection, params={"lang_id":language_id, "user_1_gender":user_1_gender_id, "user_self":1})
        
        selectionQuery = "SELECT REPLACE(`content`, %(TO_BE_REPLACED)s, %(user2_name)s) AS `content`, `theme_id`, `style_id` FROM `free_interpersonal_content` where `language_id`=%(lang_id)s AND `gender_id`=%(user_2_gender)s AND `self_user`=%(other_user)s"
        interpersonal_content_2 = pd.read_sql(selectionQuery, db_connection, params={"TO_BE_REPLACED":TO_BE_REPLACED, "user2_name":user2_name, "lang_id":language_id, "user_2_gender":user_2_gender_id , "other_user":0 })
        logger.info(difference_scores)
        try:
            for pos in ordering_list:
                content_user_1 = ''
                content_user_2 = ''
                comparison_text = ''
                diff_score = ''
                name_of_theme = ''
                
                user_1_style_code = ''
                user_2_style_code = ''

                user_1_value = str(int(pair_report_list[pos-1][0])).zfill(2)+str(int(pair_report_list[pos-1][1])).zfill(2)
                user_2_value = str(int(pair_report_list[pos-1][0])).zfill(2)+str(int(pair_report_list[pos-1][2])).zfill(2)
                
                diff_score = int((difference_scores.loc[(difference_scores['style1'].isin([int(user_1_value)]) & difference_scores['style2'].isin([int(user_2_value)])) | (difference_scores['style1'].isin([int(user_2_value)]) & difference_scores['style2'].isin([int(user_1_value)]))]['difference_score']).values[0])

                name_of_theme = str((theme_names.loc[theme_names['theme_id'].isin([pos])]['name']).values[0])
                
                comparison_text = str((scores_text.loc[scores_text['scores'].isin([diff_score])]['score_text']).values[0])
                
                user_1_style_code = user_1_value
                user_1_style_id = user_1_style_code[-2:]
                user_1_theme_id = user_1_style_code[:-2]

                logger.info("pos")
                logger.info(pos)
                logger.info("theme_name_list")
                logger.info(name_of_theme)
                logger.info("comparison_result")
                logger.info(comparison_text)
                logger.info("user_1_theme_id :")
                logger.info(user_1_theme_id)
                logger.info("user_1_style_id :")
                logger.info(user_1_style_id)
                logger.info(" language_id: ")
                logger.info(language_id)
                logger.info("user_1_gender_id :")
                logger.info(user_1_gender_id)
                
                content_user_1 = (interpersonal_content_1.loc[interpersonal_content_1['theme_id'].isin([user_1_theme_id]) & interpersonal_content_1['style_id'].isin([user_1_style_id])]['content']).values[0]
                
                user_2_style_code = user_2_value
                user_2_style_id = user_2_style_code[-2:]
                user_2_theme_id = user_2_style_code[:-2]
                
                logger.info("user_2_theme_id :")
                logger.info(user_2_theme_id)
                logger.info("user_2_style_id :")
                logger.info(user_2_style_id)
                logger.info(" language_id: ")
                logger.info(language_id)
                logger.info("user_2_gender_id :")
                logger.info(user_2_gender_id)                  
                
                content_user_2 = (interpersonal_content_2.loc[interpersonal_content_2['theme_id'].isin([user_2_theme_id]) & interpersonal_content_2['style_id'].isin([user_2_style_id])]['content']).values[0]
                
                # forming the list of
                if(diff_score != ''):
                    style_difference_scores.append(diff_score)
                else:
                    style_difference_scores.append("BLANK")
                        
                user_1_list.append(user_1_value)
                user_2_list.append(user_2_value)

                if(content_user_1 != ''):
                    user_1_content.append(content_user_1)
                else:
                    user_1_content.append("BLANK")
                
                if(content_user_2 != ''):
                    user_2_content.append(content_user_2)
                else:
                    user_2_content.append("BLANK")
                
                if(comparison_text != ''):    
                    comparison.append(comparison_text)
                else:
                    comparison.append("BLANK")    
                
                if(name_of_theme != ''):                           
                    theme.append(name_of_theme)
                else:
                    theme.append("BLANK")    
        except:
            # returning the error when there is some error in above try block
            logger.error(traceback.format_exc()) 
            return log_err(config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
    except:
        # returning the error when there is some error in above try block
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)

    return  user_1_list, user_2_list, style_difference_scores, user_1_content, user_2_content, comparison, theme

def handler(event,context):
    """Function to handle the request for upload picture API"""
    global message_by_language
    
    try:
        logger.info(event)
        # checking that the following event call is from lambda warmer or not
        if event['source']=="lambda_warmer":
            logger.info("lambda warmed")
            # returning the success json
            return {
                       'status_code':200,
                       'body':{"message":"lambda warmed"}
                   }
    except:
        # If there is any error in above operations
        pass
         
    logger.info(event)
    try:
        # Fetching data from event and rendering it
        try:
            # getting the user_id for which report need to be generated
            lambda_source = event['headers']['lambda_source']
            auth_token = event['headers']['Authorization']
            user_id_2 = event['headers']['user_id_2']
            language_id = int(event['headers']['language_id'])
            message_by_language = str(language_id) + "_MESSAGES"
        except:
            # getting the user_id for which report need to be generated
            try:
                test_token = event['headers']['test_token']
                if test_token == LOGIN_TOKEN:
                    lambda_source = "test_interface"
                    body = json.loads(event['body'])
                    user_id_1 = body['user_id_1']
                    user_id_2 = body['user_id_2']
                    user_ids = tuple([user_id_1, user_id_2])
                    language_id = int(body['language_id'])
                    message_by_language = str(language_id) + "_MESSAGES"
            
                else:
                    # if user does not have valid authorization
                    logger.error(traceback.format_exc())
                    return log_err(config[message_by_language]['UNAUTHORIZED'], 403)
            except:
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)

    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)

    if lambda_source== "invoked_lambda":
        try:
            # verifying that the user is authorized or not to see this api's data
            rid, user_id_1 = jwt_verify(auth_token)
        except:
            # if user does not have valid authorization
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['UNAUTHORIZED'], 403)
    
    try:
        try:
            # Making the DB connection
            cnx    = make_connection()
            # Getting the cursor from the DB connection to execute the queries
            cursor = cnx.cursor()

            try:
                selectionQuery = 'SELECT `id` FROM `users` WHERE `user_id`=%s'
                cursor.execute(selectionQuery, (user_id_1))
                result_list1 = []

                logger.info(result_list1)
                
                for result in cursor: result_list1.append(result)

                if result_list1 == []:
                    # If there is any error in above operations, logging the error
                    logger.error(traceback.format_exc())
                    return log_err (config[message_by_language]['INVALID_REQUEST'], 400)

                selectionQuery = 'SELECT `id` FROM `users` WHERE `user_id`=%s'
                cursor.execute(selectionQuery, (user_id_2))
                result_list2 = []

                logger.info(result_list2)
                
                for result in cursor: result_list2.append(result)

                if result_list2 == [] or result_list1 ==[]:
                    # If there is any error in above operations, logging the error
                    logger.error(traceback.format_exc())
                    return log_err (config[message_by_language]['INVALID_REQUEST'], 400)
                

            except:
                # If there is any error in above operations, logging the error
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)

            try:
                # Query for getting current language of the user
                selectionQuery = "SELECT `id`, CAST(AES_DECRYPT(`firstname`, %s) AS CHAR) FROM `users` WHERE `user_id` IN (%s, %s) ORDER BY FIELD( `user_id` , %s, %s)"
                # Executing the Query
                cursor.execute(selectionQuery, (key, user_id_1, user_id_2, user_id_1, user_id_2))
                
                result_list = []
                # fetching result from the cursor
                for result in cursor: result_list.append(result)
                
                # getting current language_id of the user
                if user_id_1 == user_id_2:
                    rid = int(result_list[0][0])
                    friends_rid = int(result_list[0][0])
                    user2_name = result_list[0][1]
                    user2_name = user2_name.capitalize()
                else:
                    rid = int(result_list[0][0])
                    friends_rid = int(result_list[1][0])
                    user2_name = result_list[1][1]
                    user2_name = user2_name.capitalize()
            except:
                # If there is any error in above operations, logging the error
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
            
        # retrieve_users_report for calculating gender
        users_report_df = retrieve_users_report([user_id_1, user_id_2], cnx)
        logger.info(users_report_df)
        # Fetching the gender of the user from the dataframe
        user_1_gender = users_report_df['gender'].iloc[0]
        user_2_gender = users_report_df['gender'].iloc[1]
        
        # Finding the correlation value between two users data
        similarity_score = calculate_similarity_score(users_report_df)

        logger.info("similarity_score :::::::::::::::::::::::")
        logger.info(similarity_score)
        logger.info("#######################################")
        
        # Fetching the theme and style related to the user
        selectionQuery = "SELECT `user_id`, `theme_id` , `style_id` FROM `user_theme_style` WHERE `user_id` IN (%s, %s) ORDER BY FIELD (`user_id`, %s, %s), `theme_id`;"
        # Execute the query
        cursor.execute(selectionQuery, (user_id_1, user_id_2, user_id_1, user_id_2))
        
        user_theme_list = []
        # fetching the result from cursor
        for result in cursor : user_theme_list.append(result)
        
        logger.info("user_theme_list :::::::::::::::::::::::")
        logger.info(user_theme_list)
        logger.info("#######################################")

        if user_id_1 == user_id_2:
            # getting pair_report_list
            pair_report_list = [[i[1], i[2], i[2]] for i in user_theme_list]

        else:
            pair_report_list = [0]*int(len(user_theme_list)/2)
            
            # Arranging the data in the required format
            for i in user_theme_list:
                if pair_report_list[int(i[1])-1]==0:
                    my_list = [0,0,0]
                else:
                    my_list = pair_report_list[int(i[1])-1]
                if i[0]==user_id_1:
                    my_list[1] = i[2]
                if i[0]==user_id_2:
                    my_list[2] = i[2]
                my_list[0] = i[1]
                pair_report_list[int(i[1])-1] = my_list

        logger.info("ans_list :::::::::::::::::::::::")
        logger.info(pair_report_list)
        logger.info("#######################################")

        ordering_list = []

        # Fetching the theme and style related to the user
        selectionQuery = "SELECT `theme_id` FROM `interpersonal_theme_order` ORDER BY `order`"
        # Execute the query
        cursor.execute(selectionQuery)
        
        # fetching result from the cursor
        for result in cursor: ordering_list.append(int(result[0]))

        logger.info(ordering_list)
        
        # calling function to format and get the final results
        user_1_list, user_2_list, style_difference_scores, user_1_content, user_2_content, comparison, theme = format_results(pair_report_list, ordering_list, user_1_gender, user_2_gender, language_id, cursor, user2_name, cnx)

        if lambda_source== "invoked_lambda":
            # Fetching the theme and style related to the user
            selectionQuery = "SELECT COUNT(*) FROM `user_interpersonal_report` WHERE `user_id` = %s and `partner_userid`= %s"
            # Execute the query
            cursor.execute(selectionQuery, (user_id_1, user_id_2))
            
            report_count = []
            
            # fetching result from the cursor
            for result in cursor: report_count.append(result)
            # Fetching data from result_list
            report_existence_count = int(report_count[0][0])
            
            if report_existence_count>0:
                # Updating the interpersonal report updated date data
                updationQuery = "UPDATE `user_interpersonal_report` SET `updated_at` = NOW() WHERE `user_id` = %s and `partner_userid`= %s"
                # Execute the query
                cursor.execute(updationQuery, (user_id_1, user_id_2))
            else:
                # Insertion of a record for user interpersonal report
                insertionQuery = "INSERT INTO `user_interpersonal_report` (`rid`,`partner_id`,`user_id`,`partner_userid`,`created_at`,`updated_at`) VALUES (%s, %s, %s, %s, NOW(), NOW())"
                # Execute the query
                cursor.execute(insertionQuery, (int(rid), int(friends_rid), user_id_1, user_id_2))
            
        logger.info({"user_1" : user_1_list, "user_2" : user_2_list, "style_difference_scores":style_difference_scores, "theme_order":ordering_list, "user_1_gender":user_1_gender, "user_2_gender":user_2_gender, 'similarity_score':similarity_score, 'user_1_content':user_1_content, 'user_2_content':user_2_content, 'comparison':comparison, 'theme':theme})
        # returning the success json to the user
        
        return {
                'statusCode': 200,
                'headers':  {
                               'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                            },
                'body': json.dumps({"user_1" : user_1_list, "user_2" : user_2_list, "style_difference_scores":style_difference_scores, "theme_order":ordering_list, "user_1_gender":user_1_gender, "user_2_gender":user_2_gender, 'similarity_score':similarity_score, 'user_1_content':user_1_content, 'user_2_content':user_2_content, 'comparison':comparison, 'theme':theme})
                }
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)


if __name__== "__main__":
    handler(None, None)