"""API Module to provide Post Response Functionalities.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. mbti_uri_string(): Generating MBTI URI based on the trait scores
4. get_trait_scores(): Retrieving the trait scores for the user
5. get_mbti_type(): Retrieving the MBTI type for the user
6. calculate_mbti_type(): Calculating MBTI type for the user based on trait scores
7. calculate_trait_scores_5(): Calculating 5 trait scores by averaging the 30 trait scores
8. calculate_trait_scores_30(): Calculating 30 trait scores with following steps:
- Retrieving the 30 scoring keys corresponding to all the questions
- Retrieving responses provided by the user for all the questions
- Multiplying user response with all the 30 scoring keys for each question
- Summing all the 30 scoring results for all the question responses to create 30 trait scores
- Normalizing the 30 trait scores to create the final 30 trait scores
- Saving these trait scores to 30 input table
9. handler(): Handling the incoming request with following tasks:
- Generating unique user_id for the user
- Creating new user record using user_id, age, gender & source IP
- Inserting user provided question responses to responses tables
- Calculating 30 & 5 trait scores based on the user responses
- Creating MBTI details for the user based on trait scores
- Returning the JSON response with user record id, affected rows and success status code

"""

import re
import uuid
import json
import pymysql
import logging
import traceback
from os import environ
import configparser
import boto3

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('postresponse.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# Variables related to s3 bucket
AWS_REGION =  environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')
PROFILES_LINK = environ.get('PROFILES_LINK')

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

def log_err(errmsg):
    """Function to log the error messages."""
    logger.error(errmsg)
    return  {
                "statusCode": 500,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }


def calculate_trait_scores_30(user_id, rid, cursor):
    """Function to calculate 30 trait scores."""
    global message_by_language
    logger.info('DEBUG: calculate_trait_scores_30() called')
    logger.info('DEBUG: user_id: ' + str(user_id))

    try:
        scoring_key = {}
        # Constructing the query to fetch the scoring keys
        query       = "SELECT * FROM `scoring_key_30`"
        # Executing the query using cursor
        cursor.execute(query)
        
        # Iterating through all the records
        for result in cursor:
            l   = list(result)
            # Removing the id from the record and storing it a variable
            uid = l.pop(0)
            # Removing the question_id from the record
            l.pop(0)

            # Creating dictionary containing key=uid and value=scores
            d2 = {uid: l}
            # Appending created dictionary to the scoring key 
            scoring_key.update(d2)
        logger.info('DEBUG: scoring_key: ' + str(scoring_key))

        responses = []
        # Constructing query to fetch all the question/responses provided by the user
        query     = "SELECT `question_id`, `question_response` FROM `user_responses` WHERE `rid` = %s"
        # Executing the query using cursor
        
        logger.info('DEBUG: question_response1: ' + str(responses))
        logger.info(query)
        logger.info(rid)
        cursor.execute(query, rid)
        logger.info('DEBUG: question_response2: ' + str(responses))

        d1        = {}
        responses = {}
        # Iterating through all the questions
        for result in cursor:
            # Creating dict where key=question_id and value=response
            d1 = {result[0]: result[1]}
            # Appending the created dictionary to responses
            responses.update(d1)
        logger.info('DEBUG: question_response: ' + str(responses))

        # Building a list of user responses after applying scoring key
        scores = []
        # Iterating through all the scoring keys
        for key in scoring_key:
            logger.info(key)
            t1 = responses[key] * scoring_key[key][0]
            t2 = responses[key] * scoring_key[key][1]
            t3 = responses[key] * scoring_key[key][2]
            t4 = responses[key] * scoring_key[key][3]
            t5 = responses[key] * scoring_key[key][4]
            t6 = responses[key] * scoring_key[key][5]
            t7 = responses[key] * scoring_key[key][6]
            t8 = responses[key] * scoring_key[key][7]
            t9 = responses[key] * scoring_key[key][8]
            t10 = responses[key] * scoring_key[key][9]
            t11 = responses[key] * scoring_key[key][10]
            t12 = responses[key] * scoring_key[key][11]
            t13 = responses[key] * scoring_key[key][12]
            t14 = responses[key] * scoring_key[key][13]
            t15 = responses[key] * scoring_key[key][14]
            t16 = responses[key] * scoring_key[key][15]
            t17 = responses[key] * scoring_key[key][16]
            t18 = responses[key] * scoring_key[key][17]
            t19 = responses[key] * scoring_key[key][18]
            t20 = responses[key] * scoring_key[key][19]
            t21 = responses[key] * scoring_key[key][20]
            t22 = responses[key] * scoring_key[key][21]
            t23 = responses[key] * scoring_key[key][22]
            t24 = responses[key] * scoring_key[key][23]
            t25 = responses[key] * scoring_key[key][24]
            t26 = responses[key] * scoring_key[key][25]
            t27 = responses[key] * scoring_key[key][26]
            t28 = responses[key] * scoring_key[key][27]
            t29 = responses[key] * scoring_key[key][28]
            t30 = responses[key] * scoring_key[key][29]
            # Keep appending to the scores list
            scores.append([t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17, t18, t19, t20, t21, t22, t23, t24, t25, t26, t27, t28, t29, t30])

        logger.info('scores_list: ' + str(scores))
        # Calculating sum of all score lists
        t = [sum(x) for x in zip(*scores)]
        logger.info('sum-of-lists: ' + str(t))
        
        # Normalizing & formatting the scores
        n = [float("{:+.3f}".format(x / 12)[:-1]) for x in t]
        logger.info('normalized_list: ' + str(n))

        # Constructing the query to insert scores to the input table
        query = "INSERT INTO `user_input_variables_30` (`user_id`, `rid`, `C1`,`C2`,`C3`,`C4`,`C5`,`C6`,`E1`,`E2`,`E3`,`E4`,`E5`,`E6`,`O1`,`O2`,`O3`,`O4`,`O5`,`O6`,`N1`,`N2`,`N3`,`N4`,`N5`,`N6`, `A1`,`A2`,`A3`,`A4`,`A5`,`A6`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        # Executing the query using cursor
        cursor.execute(query, (str(user_id), rid, float(n[0]), float(n[1]), float(n[2]), float(n[3]), float(n[4]), float(n[5]), float(n[6]), float(n[7]), float(n[8]), float(n[9]), float(n[10]), float(n[11]), float(n[12]), float(n[13]), float(n[14]), float(n[15]), float(n[16]), float(n[17]), float(n[18]), float(n[19]), float(n[20]), float(n[21]), float(n[22]), float(n[23]), float(n[24]), float(n[25]), float(n[26]), float(n[27]), float(n[28]), float(n[29])))
        
        logger.info('DONE')
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CURSOR_EXECUTION_30'])

def handler(event, context):
    """Function to handle the request for Post Response API."""
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
        # If there is any error in above operations, logging the error
        pass
        
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        
        affected_rows = 0
        old_rid = None
        try:
            old_rid = event['headers']['rid']
            if old_rid == "null":
                old_rid = None
        except:
            old_rid = None
        
        try:
            # Fetching the details from the request
            ip    = event['requestContext']['identity']['sourceIp']
            language_id = int(event['headers']['language_id'])
            message_by_language = str(language_id)+"_MESSAGES"
            logger.info(message_by_language)
            logger.info(config[message_by_language]['SUCCESS'])
            data  = event['body']
            # Getting the payload data from the request
            data  = json.loads(data)
            _uuid = None
            #logger.debug('DEBUG: data: ' + str(data))
            logger.info(data)
            total_questions = None
            total_responses = None
            is_last_page = False
            try:
                total_questions = int(event['headers']['total_questions'])
                if total_questions == "":
                    total_questions = None
                else:
                    total_questions = int(total_questions)
            except:
                total_questions = None
                
            try:
                total_responses = int(event['headers']['total_responses'])
                if total_responses== "":
                    total_responses = None
                else:
                    total_responses = int(total_responses)
            except:
                total_questions = None
                
            # First request example
            #data = '[{"gender":"male","age":39},{"qid":1,"response":-3},{"qid":2,"response":-2},{"qid":3,"response":2},{"qid":4,"response":-1},{"qid":5,"response":3},{"qid":6,"response":0},{"qid":7,"response":1},{"qid":8,"response":3},{"qid":9,"response":-2},{"qid":10,"response":2},{"qid":11,"response":-3},{"qid":12,"response":-2},{"qid":13,"response":2},{"qid":14,"response":-1},{"qid":15,"response":3},{"qid":16,"response":0},{"qid":17,"response":1},{"qid":18,"response":3},{"qid":19,"response":-2},{"qid":20,"response":2}]'
            
            # Second request example
            #data = '[{"rid":1,"qid":21,"response":-3},{"rid":1,"qid":22,"response":-2},{"rid":1,"qid":23,"response":2},{"rid":1,"qid":24,"response":-1},{"rid":1,"qid":25,"response":3},{"rid":1,"qid":26,"response":0},{"rid":1,"qid":27,"response":2},{"rid":1,"qid":28,"response":0},{"rid":1,"qid":29,"response":0},{"rid":1,"qid":30,"response":0},{"rid":1,"qid":31,"response":-1},{"rid":1,"qid":32,"response":3},{"rid":1,"qid":33,"response":2},{"rid":1,"qid":34,"response":0},{"rid":1,"qid":35,"response":-2},{"rid":1,"qid":36,"response":-3},{"rid":1,"qid":37,"response":1},{"rid":1,"qid":38,"response":3},{"rid":1,"qid":39,"response":2},{"rid":1,"qid":40,"response":-2}]'
            #data  = json.loads(data)

            # First request will not have rid (last row id from users table) yet, so generate the UUID as well
            # Iterating through all the responses in the data
            response = {}
            for response in data:
                if 'gender' in response.keys():
                    # Creating an UUID for the new user
                    _uuid = str(uuid.uuid4())
                    break
                
            is_customized = None
            is_email = None
            is_ads = None
            last_page_submitted = False
            for response in data:
                # fetching the all the three terms and condition clause value for the terms and condition clause
                if 'is_customized' in response.keys():
                    is_customized = response['is_customized']
                    is_email = response['is_email']
                    is_ads = response['is_ads']
                
            for response in data:
                if 'rid' in response.keys():
                    break
                    
            auth_token = 0
            try:
                auth_token = event['headers']['Authorization']
                if auth_token=="null":
                    auth_token = 0
            except:
                auth_token = 0
            
            # First request without rid with generated UUID
            rows              = []
            calc_trait_scores = None
            logger.info(_uuid)
            if _uuid and auth_token==0 :
                result_list = []
                query = "SELECT COUNT(*) FROM `questions_120`"
                cursor.execute(query)
                for result in cursor: result_list.append(result)
                total_questions = result_list[0][0]
                logger.info("total_questions : " + str(total_questions) + ", result_list : " + str(result_list))
                
                total_responses = -1 + len(data)
                
                query = "SELECT COUNT(*) FROM `questions_120`"
                cursor.execute(query)
                for result in cursor: result_list.append(result)
                total_questions = result_list[0][0]
                logger.info("total_questions : " + str(total_questions) + ", result_list : " + str(result_list))
                
                not_available_first_response = False
                # Iterating through all the responses in request
                for response in data:
                    
                    # If Gender & Age present in the response, we need to add this new user
                    if 'gender' in response.keys() and 'age' in response.keys():
                        if total_questions < 20:
                            is_last_page = True
                            
                        if old_rid!=None:
                            try:
                                # deleting the user when he has not completed signup process and 
                                query = "DELETE FROM `users` WHERE `id`=%s AND `is_active`!=1"
                                # Executing the cursor
                                cursor.execute(query, (int(old_rid)))
                            except:
                                # if any of the above conditon fails to execute
                                logger.error(traceback.format_exc())
                                logger.info(config[message_by_language]['USER_DELETION'])
                                
                        not_available_first_response = True
                        gender = response['gender']
                        age    = response['age']
                        # if int(gender) == 0:
                        #     sex = 'male'
                        # else:
                        #     sex = 'female'
                        # Constructing query to inserting personal details of user
                        query = "INSERT INTO `users` (`user_id`, `gender`, `age`, `ip` , `language_id`) VALUES (%s, %s, %s, %s, %s)"
                        # Executing the query using cursor
                        cursor.execute(query, (_uuid, int(gender), age, ip, language_id))
                        # Getting the last row id of the user table to store as rid
                        rid = cursor.lastrowid
                        logger.info(rid)
                        break
                
                if not_available_first_response == False:
                    return log_err(config[message_by_language]['EVENT_DATA_STATUS'])
                    
                try:
                    for response in data:
                        if 'qid' in response.keys() and 'response' in response.keys():
                            # Fetching the user responses
                            qid  = response['qid']
                            resp = response['response']
                            # Creating a record including UUID, question Id and response
                            row  = (_uuid, qid, resp, rid)
                            # Appending the each response to the rows
                            rows.append(row)
                except:
                    # If there is any error in above operations, logging the error
                    logger.error(traceback.format_exc())
                    return log_err (config[message_by_language]['SETTING_RID'])
                try:
                    # Constructing query to insert the user responses
                    query = "INSERT INTO `user_responses` (`user_id`, `question_id`, `question_response`, `rid`) VALUES (%s, %s, %s, %s)"
                    # Executing the query using cursor
                    cursor.executemany(query, rows)
                    # Storing the affected rows to be sent to client
                    affected_rows = cursor.rowcount
                    
                    # Creating the JSON response to be sent the client
                    json_body = {
                        'rid': int(rid),
                        'responses_saved': int(affected_rows),
                        'total_responses': total_responses,
                        'total_questions': total_questions,
                        'last_page':is_last_page
                    }
                    # Returing the JSON response to client
                    return {
                        'statusCode': 200,
                        'headers': {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                        },
                        'body': json.dumps(json_body)
                    }
                except:
                    # If there is any error in above operations, logging the error
                    logger.error(traceback.format_exc())
                    return log_err (config[message_by_language]['BATCH_QUERY_EXECUTION_NEW_USER'])
            
            if _uuid and auth_token!=0 and old_rid!=None:
                result_list = []
                query = "SELECT COUNT(*) FROM `questions_120`"
                cursor.execute(query)
                for result in cursor: result_list.append(result)
                total_questions = result_list[0][0]
                logger.info("total_questions : " + str(total_questions) + ", result_list : " + str(result_list))
                
                total_responses = -1 + len(data)
                
                if old_rid!=None:
                    logger.info(old_rid)
                    result_list=[]
                    # Taking count of responses given by user
                    query = "SELECT COUNT(*) FROM `user_responses` WHERE `rid`=%s"
                    # if test is not completely given
                    cursor.execute(query,(old_rid))
                    for result in cursor: result_list.append(result)
                    user_responses = result_list[0][0]
                    
                result_list=[]
                # Taking count of responses given by user
                query = "SELECT `user_id` FROM `users` WHERE `id`=%s"
                # if test is not completely given
                cursor.execute(query,(old_rid))
                for result in cursor: result_list.append(result)
                _uuid = result_list[0][0]
                not_available_first_response = False
                # Iterating through all the responses in request
                for response in data:
                    
                    # If Gender & Age present in the response, we need to add this new user
                    if 'gender' in response.keys() and 'age' in response.keys():
                        if total_questions < 20:
                            is_last_page = True
                            
                        not_available_first_response = True
                        
                        if old_rid != None:
                            
                            try:
                                # Query for deleting user responses when test is not fully completed i.e. till is_active field is not set to 1
                                deletionQuery = "DELETE FROM `user_responses` WHERE `rid`=%s AND (SELECT `is_active` FROM `users` WHERE `id`=%s)!=1"
                                # Executing the query
                                cursor.execute(deletionQuery, (int(old_rid), int(old_rid)))
                                
                                # Query for deleting description responses (output_responses) when test is not fully completed i.e. till is_active field is not set to 1
                                deletionQuery = "DELETE FROM `output_responses` WHERE `rid`=%s AND (SELECT `is_active` FROM `users` WHERE `id`=%s)!=1"
                                # Executing the query
                                cursor.execute(deletionQuery, (int(old_rid), int(old_rid)))
                            except:
                                # if any of the above conditon fails to execute
                                logger.error(traceback.format_exc())
                                return log_err (config[message_by_language]['DELETE_DATA'])
                        
                        gender = response['gender']
                        age    = response['age']
                        # if int(gender) == 0:
                        #     sex = 'male'
                        # else:
                        #     sex = 'female'
                        # Constructing query to inserting personal details of user
                        query = "UPDATE `users` SET `gender`=%s, `age`=%s, `language_id`=%s WHERE `id`=%s"
                        # Executing the query using cursor
                        cursor.execute(query, (int(gender), age, language_id, old_rid))
                        # Getting the last row id of the user table to store as rid
                        rid = cursor.lastrowid
                        break
                if not_available_first_response== False:
                    return log_err(config[message_by_language]['EVENT_DATA_STATUS'])
                
                try:
                    for response in data:
                        if 'qid' in response.keys() and 'response' in response.keys():
                            # Fetching the user responses
                            qid  = response['qid']
                            resp = response['response']
                            # Creating a record including UUID, question Id and response
                            row  = (_uuid, qid, resp, old_rid)
                            # Appending the each response to the rows
                            rows.append(row)
                except:
                    # If there is any error in above operations, logging the error
                    logger.error(traceback.format_exc())
                    return log_err (config[message_by_language]['SETTING_RID'])
                try:
                    # Constructing query to insert the user responses
                    query = "INSERT INTO `user_responses` (`user_id`, `question_id`, `question_response`, `rid`) VALUES (%s, %s, %s, %s)"
                    # Executing the query using cursor
                    cursor.executemany(query, rows)
                    # Storing the affected rows to be sent to client
                    affected_rows = cursor.rowcount
                    
                    # Creating the JSON response to be sent the client
                    json_body = {
                        'rid': int(old_rid),
                        'responses_saved': int(affected_rows),
                        'total_responses': total_responses,
                        'total_questions': total_questions,
                        'last_page' : is_last_page
                    }
                    # Returing the JSON response to client
                    return {
                        'statusCode': 200,
                        'headers': {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                        },
                        'body': json.dumps(json_body)
                    }
                except:
                    # If there is any error in above operations, logging the error
                    logger.error(traceback.format_exc())
                    return log_err (config[message_by_language]['BATCH_QUERY_EXECUTION_NEW_USER'])
                    
            # Handling the subsequent requests that contain the rid
            rows        = []
            description = False
            logger.info(data)
            logger.info(response)
            # If the request payload is not blank and contains rid
            if len(data) > 0 and 'rid' in response.keys():
                # Getting the user id from the rid
                rid   = response['rid']
                # Constructing the query to get the user_id corresponding to the rid
                query = "SELECT `user_id` FROM `users` WHERE `id` = %s"
                # Executing the query using cursor
                cursor.execute(query, (rid))
                # Getting the first record matching the rid
                result = list(cursor.fetchone())
                # Storing the user id
                _uuid  = result[0]
                logger.info(language_id)
                logger.info(rid)
                query = "UPDATE `users` set `language_id`=%s WHERE `id`=%s"
                # Executing the query using cursor
                cursor.execute(query, (int(language_id), int(rid)))
                
                # Iterating through the responses in the request
                for response in data:
                    if 'rid' in response:
                        # Fetching the user responses
                        rid  = response['rid']
                        qid  = response['qid']
                        resp = response['response']
                        # Creating a record including UUID, question Id and response
                        row  = (_uuid, qid, resp, rid)
                        # Appending the each response to the rows
                        rows.append(row)
                    
                    # If description is repsent in the response, then set the flag
                    if 'description' in response.keys():
                        description = True
                
                if total_responses:
                    total_responses = total_responses + len(data)
                    # If this is the last set of questions
                    if total_responses >= total_questions:
                        logger.debug('DEBUG: found id 120')
                        logger.debug('DEBUG: calling calculate_trait_scores() uuid: ' + _uuid)
                        # Setting the flag to calculate trait score
                        last_page_submitted = True
                
                    if total_questions%20 > 0:
                        last_page_index = total_questions - total_questions%20
                    elif total_questions%20 == 0:
                        last_page_index = total_questions - 20
                
                    if total_responses == last_page_index:
                        is_last_page = True
                
                if is_customized != None:
                    # when is_customized field is there than inserting the user permission
                    logger.info(int(rid))
                    logger.info(_uuid)
                    logger.info(int(is_customized))
                    logger.info(int(is_email))
                    logger.info(int(is_ads))
                    
                    try:
                        # inserting user permission of a user
                        insertionQuery = "INSERT INTO `user_permissions` (`rid`, `user_id`, `is_customized`, `is_email`, `is_ads`) VALUES (%s, %s, %s, %s, %s)"
                        # Executing the query using cursor
                        cursor.execute(insertionQuery, (int(rid), _uuid, int(is_customized), int(is_email), int(is_ads)))
                    except:
                        # If there is any error in above operations, logging the error
                        logger.error(traceback.format_exc())
                        return log_err(config[message_by_language]['TERMS_CONDITION_STATUS'])            
                    
                    if int(is_customized)==1:
                        logger.debug('DEBUG: calling calculate_trait_scores() uuid: ' + _uuid)
                        # Setting the flag to calculate trait score
                        calc_trait_scores = True
                
                    
                try:
                    # Constructing query to insert the user responses
                    if description:
                        query = "INSERT INTO `output_responses` (`user_id`, `output_id`, `output_value`, `rid`) VALUES (%s, %s, %s, %s)"
                    else:
                        query = "INSERT INTO `user_responses` (`user_id`, `question_id`, `question_response`, `rid`) VALUES (%s, %s, %s, %s)"
                        
                    # Executing the query using cursor
                    cursor.executemany(query, rows)
                    # Storing the affected rows to be sent to client
                    affected_rows = cursor.rowcount
                    
                    mbti_uri = None
                    # If calculate trait scores flag is on
                    if calc_trait_scores:
                        logger.debug('DEBUG: calling calculate_trait_scores_30()')
                        # Calculating the 30 trait scores
                        trait_scroes_30 = calculate_trait_scores_30(_uuid, rid, cursor)
                        
                        try:
                            # inserting user permission of a user
                            updationQuery = "UPDATE `users` SET `is_active`=1 WHERE `primary_email` IS NOT NULL AND `id` IN (SELECT `rid` FROM `user_permissions` WHERE `is_customized`=1 AND `rid`=%s)"
                            # Executing the query using cursor
                            cursor.execute(updationQuery, (int(rid)))
                            
                            # getting the is_active field from the users table to check that the test is complete or not
                            selectionQuery = "SELECT `is_active`,`user_id`, `language_id` FROM `users` WHERE `id`=%s"
                            # Executing the query
                            cursor.execute(selectionQuery, (int(rid)))
                            user_data = []
                            # getting the result list from the cursor
                            for result in cursor: user_data.append(result)
                            # getting the value of is_active field from the list
                            is_active = int(user_data[0][0])
                            user_id = user_data[0][1]
                            language_id = int(user_data[0][2])
                        except:
                            # If there is any error in above operations, logging the error
                            logger.error(traceback.format_exc())
                            return log_err(config[message_by_language]['IS_ACTIVE_STATUS'])
                            
                        try:
                            # creating a boto3 service client object
                            invokeLam = boto3.client(
                                                        "lambda", 
                                                        region_name=AWS_REGION,
                                                        aws_access_key_id=AWS_ACCESS_KEY,
                                                        aws_secret_access_key=AWS_SECRET
                                                    )
                        except:
                            # when there is some problem in above code
                            logger.error(traceback.format_exc())
                            return log_err (config[message_by_language]['BOTO_SERVICE_CLIENT_STATUS'])
                        
                        if is_active==1:
                            
                            try:
                                payload = {'rid':int(rid)}
                                invokeLam.invoke(FunctionName="ProfilesSendNotificationsToFriendsAsync" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
                                
                                # preparing payload for the lambda call
                                payload = {'rid' : int(rid)}
                                
                                # calling the lambda function asynchronously to scrape the users profile
                                invokeLam.invoke(FunctionName="ProfilesFacebookScrapeImage" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
                            except:
                                # when there is some problem in above code
                                logger.error(traceback.format_exc())
                                return log_err (config[message_by_language]['INVOKING_ASYNC_STATUS'])
                        else:
                            try:
                                # adding self_user, section_id (all sections), report_tab default values
                                self_user = 1
                                section_id = -1
                                report_tab = "INDIVIDUAL_FILTERED_V1"
                                # asynchronous function to be called when trait scores are calculated and calling api to generate user_profile_report
                                payload = {"headers":{"user_id":user_id, "language_id":language_id, "lambda_source":"invoked_lambda", "report_tab": report_tab, "self_user": self_user, "section_id": section_id}}
                                invokeLam.invoke(FunctionName="ProfilesGenerateUserProfileReport" + ENVIRONMENT_TYPE, InvocationType="Event", Payload=json.dumps(payload))
                            except:
                                # when there is some problem in above code
                                logger.error(traceback.format_exc())
                                return log_err (config[message_by_language]['INVOKING_ASYNC_STATUS'])
                    
                    # If MTBI details are fetched, add this to JSON response, otherwise don't
                    if last_page_submitted:
                        json_body = {
                            'rid': int(rid),
                            'responses_saved': int(affected_rows),
                            'total_responses': total_responses,
                            'total_questions': total_questions,
                            'message':'Description'
                    }
                    else:
                        json_body = {
                            'rid': int(rid),
                            'responses_saved': int(affected_rows),
                            'total_responses': total_responses,
                            'total_questions': total_questions,
                            'last_page' : is_last_page
                    }
                    
                    # Returning JSON response
                    return {
                        'statusCode': 200,
                        'headers': {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                        },
                        'body': json.dumps(json_body)
                    }
                    cursor.close()
                except:
                    # If there is any error in above operations, logging the error
                    logger.error(traceback.format_exc())
                    return log_err (config[message_by_language]['BATCH_QUERY_EXECUTION_EXISTING_USER'])
        
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['CURSOR_EXECUTION_HANDLER'])
        
        # If none of the above conditions occur, return the default response
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'
            },
            'body': json.dumps({'responses_saved':str(affected_rows)})
        }
        
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS_HANDLER'])


    finally:
        try:
            # Finally, clean up the connection
            cnx.close()
        except:
            pass

if __name__== "__main__":
    handler(None,None)