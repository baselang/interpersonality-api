"""
API for generating User Guides Report

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): For decoding the authorization token
4. find_section_theme(): getting section from theme
5. get_guide_report(): generating guide report
6. get_style_pairing_importance(): getting style pairing importance
7. get_section_and_theme_wise_style_pair_summaries(): getting section and theme wise style pair
8. handler(): Handling the incoming request with following steps
- Fetching the data from the database
- processing the data and then generating the guides report for the users pair
- returning the success from the method

"""

import pandas as pd
from os import environ
import pymysql
import numpy as np
import math
import traceback
import configparser
import json
import logging
import time
import jwt

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('user_guides_report.properties', encoding = "ISO-8859-1")

# getting the variable for storing the default message language
message_by_language = environ.get('MESSAGES_LANGUAGE')

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

# fetching the variables that are essential for the api
theme_order = environ.get('THEME_ORDER')

#Settting a parameters of Section 0 condition
MAX_SUMMARIES = int(environ.get('MAX_SUMMARIES'))
MIN_POSITIVES = int(environ.get('MIN_POSITIVES'))
MAX_NEGATIVES = int(environ.get('MAX_NEGATIVES'))

# Login Token
LOGIN_TOKEN = environ.get('LOGIN_TOKEN')

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
    return rid, user_id, language_id

# Function to find element in the list
def findElement(listName, searchElement):
    for value in listName:
        user_1_style_id = value[-2:]
        user_1_theme_id = value[:-2]
        if(int(user_1_theme_id) == searchElement):
            return value
    return '0'    

def find_section_theme(user_1_list,user_2_list,themes_in_sections_input_data,report_name):
    """Find Section number related to theme"""

    #Create dictionary
    SECTION_AND_THEME_DICT = dict()

    #Reassign theme_num values.
    theme_num = list(range(1, 16))

    #Finding section using theme.
    for theme_id in theme_num:

        #Finding user_1_style, user_2_style based on theme_id
        user_1_style = findElement(user_1_list, int(theme_id))
        user_2_style = findElement(user_2_list, int(theme_id))


        if((user_1_style =='0') & (user_2_style=='0')):
            continue

        user_1_style_id = user_1_style[-2:]
        user_2_style_id = user_2_style[-2:]

        themes_wise_sections_input_data =  themes_in_sections_input_data.loc[( themes_in_sections_input_data['theme_id'] == int(theme_id))]

        themes_in_sections = pd.DataFrame()

        #CASE 1.  USER 1 STYLE ID , USER 2 STYLE ID                    
        themes_in_sections =  themes_wise_sections_input_data.loc[( themes_wise_sections_input_data['style_id_1'] == int(user_1_style_id))
        &  ( themes_wise_sections_input_data['style_id_2'] == int(user_2_style_id))]

        if(report_name == 'CouplesGuide'):
            #CASE 2.  USER 2 STYLE ID , USER 1 STYLE ID                    
            if(themes_in_sections.empty):
                themes_in_sections =  themes_wise_sections_input_data.loc[( themes_wise_sections_input_data['style_id_1'] == int(user_2_style_id))
                &  ( themes_wise_sections_input_data['style_id_2'] == int(user_1_style_id))]

        #CASE 3 USER 1 STYLE ID, BLANK                    
        if(themes_in_sections.empty):
            themes_in_sections =  themes_wise_sections_input_data.loc[( themes_wise_sections_input_data['style_id_1'] == int(user_1_style_id))
        &  ( themes_wise_sections_input_data['style_id_2'] == -1)]

        if(report_name == 'CouplesGuide'):
            #CASE 4 BLANK, USER 1 STYLE ID                    
            if(themes_in_sections.empty):
                themes_in_sections =  themes_wise_sections_input_data.loc[( themes_wise_sections_input_data['style_id_1'] == -1)
            &  ( themes_wise_sections_input_data['style_id_2'] == int(user_1_style_id))]


        #CASE 5 BLANK, USER 2 STYLE ID                    
        if(themes_in_sections.empty):
            themes_in_sections =  themes_wise_sections_input_data.loc[( themes_wise_sections_input_data['style_id_1'] == -1)
        &  ( themes_wise_sections_input_data['style_id_2'] == int(user_2_style_id))]

        if(report_name == 'CouplesGuide'):
            #CASE 6 USER 2 STYLE ID, BLANK                    
            if(themes_in_sections.empty):
                themes_in_sections =  themes_wise_sections_input_data.loc[( themes_wise_sections_input_data['style_id_1'] == int(user_2_style_id))
            &  ( themes_wise_sections_input_data['style_id_2'] == -1)]

        #CASE 7 BLANK, BLANK                    
        if(themes_in_sections.empty):
            themes_in_sections =  themes_wise_sections_input_data.loc[( themes_wise_sections_input_data['style_id_1'] == -1)
        &  ( themes_in_sections_input_data['style_id_2'] == -1)]


        if(themes_in_sections.empty):
            logger.info("themes_in_sections empty")
        else:
            section = themes_in_sections['section_id'].values[0]

        theme = []

        try:        
            theme = SECTION_AND_THEME_DICT[section]
            theme.append(theme_id)
        except:
            theme.append(theme_id)
            
        SECTION_AND_THEME_DICT.update({section: theme})
        
    return SECTION_AND_THEME_DICT


def get_section_and_theme_wise_style_pair_summaries(section_number,theme_id,user_1_style_id,user_2_style_id,usage,report_name,style_pair_summaries_input_data):
    """Get section_and_theme_wise_style_pair_summaries"""  
    section_and_theme_wise_style_pair_summaries = pd.DataFrame()
    
    #CASE 1.  USER 1 STYLE ID , USER 2 STYLE ID                    
    section_and_theme_wise_style_pair_summaries = style_pair_summaries_input_data.loc[(style_pair_summaries_input_data['section_id'] == int(section_number))
    &  (style_pair_summaries_input_data['theme_id'] == int(theme_id))
    &  (style_pair_summaries_input_data['style_id_1'] == int(user_1_style_id))
    &  (style_pair_summaries_input_data['style_id_2'] == int(user_2_style_id))
    &  (style_pair_summaries_input_data['usage'] == int(usage))]
    
    if(report_name == 'CouplesGuide'):
        #CASE 2.  USER 2 STYLE ID , USER 1 STYLE ID                    
        if(section_and_theme_wise_style_pair_summaries.empty):
            section_and_theme_wise_style_pair_summaries = style_pair_summaries_input_data.loc[(style_pair_summaries_input_data['section_id'] == int(section_number))
            &  (style_pair_summaries_input_data['theme_id'] == int(theme_id))
            &  (style_pair_summaries_input_data['style_id_1'] == int(user_2_style_id))
            &  (style_pair_summaries_input_data['style_id_2'] == int(user_1_style_id))
            &  (style_pair_summaries_input_data['usage'] == int(usage))]

    #CASE 3 USER 1 STYLE ID, BLANK                    
    if(section_and_theme_wise_style_pair_summaries.empty):
        section_and_theme_wise_style_pair_summaries = style_pair_summaries_input_data.loc[(style_pair_summaries_input_data['section_id'] == int(section_number))
    &  (style_pair_summaries_input_data['theme_id'] == int(theme_id))
    &  (style_pair_summaries_input_data['style_id_1'] == int(user_1_style_id))
    &  (style_pair_summaries_input_data['style_id_2'] == -1)                            
    &  (style_pair_summaries_input_data['usage'] == int(usage))]

    if(report_name == 'CouplesGuide'):
        #CASE 4 BLANK, USER 1 STYLE ID                    
        if(section_and_theme_wise_style_pair_summaries.empty):
            section_and_theme_wise_style_pair_summaries = style_pair_summaries_input_data.loc[(style_pair_summaries_input_data['section_id'] == int(section_number))
        &  (style_pair_summaries_input_data['theme_id'] == int(theme_id))
        &  (style_pair_summaries_input_data['style_id_1'] == -1)
        &  (style_pair_summaries_input_data['style_id_2'] == int(user_1_style_id))                            
        &  (style_pair_summaries_input_data['usage'] == int(usage))]


    #CASE 5 BLANK, USER 2 STYLE ID                    
    if(section_and_theme_wise_style_pair_summaries.empty):
        section_and_theme_wise_style_pair_summaries = style_pair_summaries_input_data.loc[(style_pair_summaries_input_data['section_id'] == int(section_number))
    &  (style_pair_summaries_input_data['theme_id'] == int(theme_id))
    &  (style_pair_summaries_input_data['style_id_1'] == -1)
    &  (style_pair_summaries_input_data['style_id_2'] == int(user_2_style_id))                            
    &  (style_pair_summaries_input_data['usage'] == int(usage))]


    if(report_name == 'CouplesGuide'):
        #CASE 6 USER 2 STYLE ID, BLANK                    
        if(section_and_theme_wise_style_pair_summaries.empty):
            section_and_theme_wise_style_pair_summaries = style_pair_summaries_input_data.loc[(style_pair_summaries_input_data['section_id'] == int(section_number))
        &  (style_pair_summaries_input_data['theme_id'] == int(theme_id))
        &  (style_pair_summaries_input_data['style_id_1'] == int(user_2_style_id))
        &  (style_pair_summaries_input_data['style_id_2'] == -1)                            
        &  (style_pair_summaries_input_data['usage'] == int(usage))]

    #CASE 7 BLANK, BLANK                    
    if(section_and_theme_wise_style_pair_summaries.empty):                                
        section_and_theme_wise_style_pair_summaries = style_pair_summaries_input_data.loc[(style_pair_summaries_input_data['section_id'] == int(section_number))
    &  (style_pair_summaries_input_data['theme_id'] == int(theme_id))
    &  (style_pair_summaries_input_data['style_id_1'] == -1)
    &  (style_pair_summaries_input_data['style_id_2'] == -1)
    &  (style_pair_summaries_input_data['usage'] == int(usage))]

    return section_and_theme_wise_style_pair_summaries


def get_style_pairing_importance(theme_id,user_1_style_id,user_2_style_id,report_name,style_pairing_importance_input_data):
    """Get style_pairing_importance"""
    #CASE 1. USER 1 STYLE ID , USER 2 STYLE ID
    style_pairing_importance_df = style_pairing_importance_input_data.loc[(style_pairing_importance_input_data['theme'] == int(theme_id))
    &  (style_pairing_importance_input_data['user_style_1'] == int(user_1_style_id))
    &  (style_pairing_importance_input_data['user_style_2'] == int(user_2_style_id))]

    if(report_name == 'CouplesGuide'):
        #CASE 2. USER 2 STYLE ID , USER 1 STYLE ID
        if(style_pairing_importance_df.empty):
            style_pairing_importance_df = style_pairing_importance_input_data.loc[(style_pairing_importance_input_data['theme'] == int(theme_id))
        &  (style_pairing_importance_input_data['user_style_1'] == int(user_2_style_id))
        &  (style_pairing_importance_input_data['user_style_2'] == int(user_1_style_id))]

    #CASE 3. USER 1 STYLE ID , BLANK
    if(style_pairing_importance_df.empty):
        style_pairing_importance_df = style_pairing_importance_input_data.loc[(style_pairing_importance_input_data['theme'] == int(theme_id))
    &  (style_pairing_importance_input_data['user_style_1'] == int(user_1_style_id))
    &  (style_pairing_importance_input_data['user_style_2'] == -1)]

    if(report_name == 'CouplesGuide'):
        #CASE 4. BLANK, USER 1 STYLE ID
        if(style_pairing_importance_df.empty):
            style_pairing_importance_df = style_pairing_importance_input_data.loc[(style_pairing_importance_input_data['theme'] == int(theme_id))
        &  (style_pairing_importance_input_data['user_style_1'] == -1)
        &  (style_pairing_importance_input_data['user_style_2'] == int(user_1_style_id))]


    #CASE 5. BLANK, USER 2 STYLE ID
    if(style_pairing_importance_df.empty):
        style_pairing_importance_df = style_pairing_importance_input_data.loc[(style_pairing_importance_input_data['theme'] == int(theme_id))
    &  (style_pairing_importance_input_data['user_style_1'] == -1)
    &  (style_pairing_importance_input_data['user_style_2'] == int(user_2_style_id))]

    if(report_name == 'CouplesGuide'):
        #CASE 6. USER 2 STYLE ID, BLANK
        if(style_pairing_importance_df.empty):
            style_pairing_importance_df = style_pairing_importance_input_data.loc[(style_pairing_importance_input_data['theme'] == int(theme_id))
        &  (style_pairing_importance_input_data['user_style_1'] == int(user_2_style_id))
        &  (style_pairing_importance_input_data['user_style_2'] == -1)]

    #CASE 7. BLANK, BLANK
    if(style_pairing_importance_df.empty):
        style_pairing_importance_df = style_pairing_importance_input_data.loc[(style_pairing_importance_input_data['theme'] == int(theme_id))
    &  (style_pairing_importance_input_data['user_style_1'] == -1)
    &  (style_pairing_importance_input_data['user_style_2'] == -1)]

    return style_pairing_importance_df

def get_guide_report(user_1_list, user_2_list, firstname_1, firstname_2, gender_1, gender_2, report_name, section_num, language_id, cnx, cursor, final_json, product_id):

    """Get guide_report"""
    available_summaries = []
    summaries_to_display = []

    theme_id = ''
    section_id  = ''
    usage  = ''
    positive  = ''
    content  = ''  

    section_dataframe_list = []
    dataframe_list = []

    POSITIVE_COUNT = 0
    NEGATIVE_COUNT = 0

    #Reading input CSV file

    selectionQuery1 = "SELECT `id`, `section_no` AS  `section_id` , `theme_id`,`style_id_1` AS `user_style_1`, `style_id_2` AS `user_style_2` FROM `guide_section_styles` WHERE `product_id`={}".format(product_id)
    selectionQuery2 = "SELECT `id`, `theme_id` AS `theme`, `style_id_1` AS `user_style_1`, `style_id_2` AS `user_style_2`, `importance` FROM `guide_style_pair_importance` WHERE `product_id`={}".format(product_id)
    selectionQuery3 = "SELECT `id` AS `Id`, `theme_id`, `style_id_1`, `style_id_2`, `section_no` AS `section_id`, `usage`, `positive`, `language_id`, `content`  FROM `guide_style_pair_summaries` WHERE `product_id` = {} AND `language_id`={}".format(product_id, language_id)
    selectionQuery4 = "SELECT `id` AS `Id`, `theme_id`, `style_id`, `section_no` AS `section_id`,`language_id`, `gender_id`, `self_user`, `content` FROM `guide_profile_content` WHERE `product_id` = {} AND `language_id`={}".format(product_id, language_id)
    
    themes_in_sections_input_data = pd.read_sql(selectionQuery1, cnx)
    style_pairing_importance_input_data = pd.read_sql(selectionQuery2, cnx)
    style_pair_summaries_input_data = pd.read_sql(selectionQuery3, cnx)
    individual_style_summaries_input_data = pd.read_sql(selectionQuery4, cnx)
    
    # Query for getting the theme name
    selectionQuery = "SELECT `name`, `theme_id` FROM `theme_names` WHERE `language_id`={}".format(language_id)
    theme_names = pd.read_sql(selectionQuery, cnx)
    
    # Rename column user_style_1 and user_style_2
    themes_in_sections_input_data = themes_in_sections_input_data.rename(columns = {'user_style_1':'style_id_1'}).rename(columns = {'user_style_2':'style_id_2'})

    SECTION_AND_THEME_DICT = dict()

    #Find section in theme
    SECTION_AND_THEME_DICT  = find_section_theme(user_1_list,user_2_list,themes_in_sections_input_data,report_name)

    #Iterate sections 0 to 4
    for section_number in section_num:
        if(section_number != 0):
            #Find section associated theme 
            theme = SECTION_AND_THEME_DICT[section_number]

            #Iterate theme
            for theme_id in theme:
                theme_name = ''
                preframe_style = ''
                style_comparison_self_user = ''
                style_comparison_other_user = ''
                outro_text_style = ''
                
                # Query for getting the theme name
                selectionQuery = "SELECT `name` FROM `theme_names` WHERE `language_id`=%s AND `theme_id`=%s"                
                cursor.execute(selectionQuery, (int(language_id), int(theme_id)))
        
                theme_name_list = []
                # fetching the result from the cursor
                for themeName in cursor:                                 
                    theme_name = ''.join(themeName)

                #Finding user_1_style, user_2_style based on theme_id
                user_1_style = findElement(user_1_list, int(theme_id))
                user_2_style = findElement(user_2_list, int(theme_id))

                #If user_1_style and user_2_style not available skip process.
                if((user_1_style =='0') & (user_2_style=='0')):
                    continue

                user_1_style_id = user_1_style[-2:]
                user_2_style_id = user_2_style[-2:]

                # Usage 1 for Preframe style
                usage = 1
                
                section_and_theme_wise_style_pair_summaries = pd.DataFrame()

                # Get section_and_theme_wise_style_pair_summaries dataframe
                section_and_theme_wise_style_pair_summaries = get_section_and_theme_wise_style_pair_summaries(section_number,theme_id,user_1_style_id,user_2_style_id,usage,report_name,style_pair_summaries_input_data)
            
                #Preframe style pair summary is NULL skip process
                if(section_and_theme_wise_style_pair_summaries.empty):
                    continue         

                #Check theme, user style , section, language, self user in individual style summaries
                self_user_individual_style_summaries_input_data = individual_style_summaries_input_data.loc[(individual_style_summaries_input_data['section_id'] == int(section_number))
                &  (individual_style_summaries_input_data['theme_id'] == int(theme_id))
                &  (individual_style_summaries_input_data['style_id'] == int(user_1_style_id))
                &  (individual_style_summaries_input_data['gender_id'] == int(gender_1))
                &  (individual_style_summaries_input_data['self_user'] == int(1))]  

                #check self user individual style summaries is NULL skip process 
                if(self_user_individual_style_summaries_input_data.empty):
                    continue

                #Check theme, user style , section, language, other user in individual style summaries
                other_user_individual_style_summaries_input_data = individual_style_summaries_input_data.loc[(individual_style_summaries_input_data['section_id'] == int(section_number))
                &  (individual_style_summaries_input_data['theme_id'] == int(theme_id))
                &  (individual_style_summaries_input_data['style_id'] == int(user_2_style_id))
                &  (individual_style_summaries_input_data['gender_id'] == int(gender_2))
                &  (individual_style_summaries_input_data['self_user'] == 0)]

                #check other user individual style summaries is NULL skip process
                if(other_user_individual_style_summaries_input_data.empty):
                    continue

                # Usage 2 for outro style
                usage = 2                    
                section_and_theme_wise_outro_style_pair_summaries = pd.DataFrame()
                section_and_theme_wise_outro_style_pair_summaries = get_section_and_theme_wise_style_pair_summaries(section_number,theme_id,user_1_style_id,user_2_style_id,usage,report_name,style_pair_summaries_input_data)                
                             
                style_pairing_importance_df = pd.DataFrame()                    
                style_pairing_importance_df = get_style_pairing_importance(theme_id,user_1_style_id,user_2_style_id,report_name,style_pairing_importance_input_data)                
                
                if(section_and_theme_wise_outro_style_pair_summaries.empty):
                    logger.info("section_and_theme_wise_outro_style_pair_summaries empty")
                else:
                    outro_text_style = section_and_theme_wise_outro_style_pair_summaries['content'].values[0]

                #If style_pairing_importance_df is empty assign default value 0 for sorting.
                if(style_pairing_importance_df.empty):
                    importance = 0
                else:
                    importance = style_pairing_importance_df['importance'].values[0]
                    

                preframe_style = section_and_theme_wise_style_pair_summaries['content'].values[0]
                style_comparison_self_user = self_user_individual_style_summaries_input_data['content'].values[0]
                style_comparison_other_user = other_user_individual_style_summaries_input_data['content'].values[0]
                
                dataframe_row_list = []
                dataframe_row_list = [section_number,str(theme_id),importance,theme_name, preframe_style,style_comparison_self_user,style_comparison_other_user,outro_text_style,user_1_style_id,user_2_style_id,user_1_style,user_2_style]
                dataframe_list.append(dataframe_row_list)

    # Convert list to dataframe                               
    couples_guide_df = pd.DataFrame(dataframe_list)
    if(couples_guide_df.empty):
        logger.info("couples_guide_df is empty")
    else:
        couples_guide_df.columns = ["section","theme_id","importance","theme_name","preframe_style","style_comparison_self_user","style_comparison_other_user","outro_text_style","user_1_style_id","user_1_style_id","user_1_style","user_2_style"]
        couples_guide_df.sort_values(["section","importance"], axis=0, ascending=[True,False], inplace=True)                     
    
    final_json_keys = list(final_json.keys())
    
    logger.info("final_json_keys ::::::::::::::::::::::::")
    logger.info(final_json_keys)
    
    final_json_keys.sort(key=len, reverse=True)
    
    logger.info("final_json_keys ::::::::::::::::::::::::")
    logger.info(final_json_keys)
    
    guides_report = []
    
    # iterating over the couples guide each entry
    for i in couples_guide_df.values.tolist():
        # Fetching each record in the json
        preframe_style = i[4]
        style_comparison_self_user = i[5]
        style_comparison_other_user = i[6]
        outro_text_style = i[7]
        
        # iterating over each key in final json
        for j in final_json_keys:
            # if text similar to key found in the content then replacing it
            preframe_style = preframe_style.replace(j, final_json[j])
            style_comparison_self_user = style_comparison_self_user.replace(j, final_json[j])
            style_comparison_other_user = style_comparison_other_user.replace(j, final_json[j])
            outro_text_style = outro_text_style.replace(j, final_json[j])
           
        # appending the record into the couples_guide json
        guides_report.append({"section" : i[0],"theme_id": i[1], "importance" : i[2], "theme_name" : i[3], "preframe_style" : preframe_style, "style_comparison_self_user" : style_comparison_self_user, "style_comparison_other_user" : style_comparison_other_user, "outro_text_style" : outro_text_style, "user_1_style_id" : i[8], "user_2_style_id" : i[9], "user_1_style" : i[10], "user_2_style" : i[11]})

    # Return json data
    return guides_report


def handler(event,context):
    """Function to handle the request for generating guides report API"""
    global message_by_language
    logger.info(event)
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
         
    try:
        auth_token = None
        user_partner_id = None
        try:
            # getting the user_id for which report need to be generated
            user_partner_id = int(event['headers']['user_partner_id'])
            auth_token = event['headers']['Authorization']
        except:
            # getting the user_id for which report need to be generated
            test_token = event['headers']['test_token']
            
            if test_token != LOGIN_TOKEN:
                # if user does not have valid token
                logger.info(config[message_by_language]['TEST_TOKEN_STATUS'])
                return log_err(config[message_by_language]['UNAUTHORIZED'], 403)
                
            body = json.loads(event['body'])
            user_id_1 = body['user_id_1']
            user_id_2 = body['user_id_2']
            user_ids = tuple([user_id_1, user_id_2])
            language_id = int(body['language_id'])
            report_name = body['report_name']
            message_by_language = str(language_id) + "_MESSAGES"
    except:
        # returning the error when there is some error in above try block
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
    
    if auth_token != None:
        try:
            # verifying that the user is authorized or not to see this api's data
            rid, user_id, language_id = jwt_verify(auth_token)
            message_by_language = str(language_id) + "_MESSAGES"
        except:
            # if user does not have valid authorization
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['UNAUTHORIZED'], 403)
    
    try:
        rid_1 = None
        rid_2 = None
        firstname_1 = None
        firstname_2 = None
        is_coaching = False
        try:
            # Making the DB connection
            cnx    = make_connection()
            # Getting the cursor from the DB connection to execute the queries
            cursor = cnx.cursor()

            try:
                if auth_token != None and user_partner_id != None:
                    
                    # Query for getting the user_id and rid related to the product purchase from user_partner_products
                    selectionQuery = "SELECT `user_rid`, `partner_rid`, `product_id` FROM `user_partner_products` WHERE `id` = %s"
                    # Executing the Query
                    cursor.execute(selectionQuery, (user_partner_id))
                    
                    result_list = []
                    
                    for result in cursor: result_list.append(result)
                    
                    if result_list == []:
                        # returning the invalid access since user is unauthorized to access these product
                        return log_err(config[message_by_language]['NOT_FOUND'], 404)
                    
                    rid_1 = int(result_list[0][0])
                    rid_2 = int(result_list[0][1])
                    product_id = int(result_list[0][2])
                    
                    # finding the report_name on the basis of product_id
                    if product_id == 2:
                        report_name = "CouplesGuide"
                    elif product_id == 1:
                        report_name = "CompatibilityGuide"
                        
                    if rid_1 != rid:
                        # If the user who is requesting guide data is not the valid user
                        logger.info(config[message_by_language]['USER_STATUS'])
                        return log_err(config[message_by_language]['UNAUTHORIZED'], 403)
                        
                    # Query for getting name of the user
                    selectionQuery = "SELECT `id`, CAST(AES_DECRYPT(`firstname`, %s) AS CHAR), `picture_url`, `gender`, CAST(AES_DECRYPT(`primary_email`, %s) AS CHAR), CAST(AES_DECRYPT(`name`, %s) AS CHAR), `language_id` FROM `users` WHERE `id` IN (%s, %s)"
                    # Executing the Query
                    cursor.execute(selectionQuery, (key, key, key, rid_1, rid_2))
                    
                    result_list = []
                    # fetching result from the cursor
                    for result in cursor: result_list.append(result)
                    
                    language_id = None
                    primary_email = None
                    name = None
                    
                    # getting current name related to the user of the user
                    for i in result_list:
                        if int(i[0]) == rid_1:
                            firstname_1 = i[1]
                            picture_url = i[2]
                            gender_1 = int(i[3])
                            language_id = int(i[6])
                            primary_email = i[4]
                            name = i[5]
                            message_by_language = str(language_id) + "_MESSAGES"
                        elif int(i[0]) == rid_2:
                            firstname_2 = i[1]
                            partner_picture_url = i[2]
                            gender_2 = int(i[3])
                else:
                    # Query for getting current language of the user
                    selectionQuery = "SELECT `id`, CAST(AES_DECRYPT(`firstname`, %s) AS CHAR), `gender` FROM `users` WHERE `user_id` IN (%s, %s) ORDER BY FIELD( `user_id` , %s, %s)"
                    # Executing the Query
                    cursor.execute(selectionQuery, (key, user_id_1, user_id_2, user_id_1, user_id_2))
                    
                    result_list = []
                    # fetching result from the cursor
                    for result in cursor: result_list.append(result)
                    
                    try:
                        # getting current language_id of the user
                        if user_id_1 == user_id_2:
                            rid_1 = int(result_list[0][0])
                            rid_2 = int(result_list[0][0])
                            firstname_1 = result_list[0][1]
                            firstname_2 = result_list[0][1]
                            gender_1 = int(result_list[0][2])
                            gender_2 = int(result_list[0][2])
                        else:
                            rid_1 = int(result_list[0][0])
                            rid_2 = int(result_list[1][0])
                            firstname_1 = result_list[0][1]
                            firstname_2 = result_list[1][1]
                            gender_1 = int(result_list[0][2])
                            gender_2 = int(result_list[1][2])
                    except:
                        # If there is any error in above operations, logging the error
                        logger.error(traceback.format_exc())
                        return log_err (config[message_by_language]['INVALID_REQUEST'], 400)
            except:
                # If there is any error in above operations, logging the error
                logger.error(traceback.format_exc())
                return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
        
        first_user_json = {}
        second_user_json = {}
        final_json = {"SELFNAME_1":firstname_1,"OTHERNAME_1":firstname_2, "SELFNAME_0":firstname_2, "OTHERNAME_0":firstname_1, "USERNAME_1":firstname_1, "USERNAME_2":firstname_2}
        
        if gender_1 != gender_2:
            
            # fetching key and value from guide_name_parameters according to language_id and gender of the user for gender 1
            selectionQuery = "SELECT `name_key`, `name_value`, `gender` FROM `guide_name_parameters` WHERE `language_id` = %s AND `gender` IN (%s, %s)"
            # Executing the query
            cursor.execute(selectionQuery, (language_id, gender_1, gender_2))
            
            for result in cursor:
                if result[0].find("_OTHERNAME") != -1 and  result[2] == gender_1:
                    final_json[result[0] + "_0"] = result[1]
                elif result[0].find("_user") != -1 and result[2] == gender_1:
                    final_json[result[0] + "_1"] = result[1]
                    first_user_json[result[0]] = result[1]
                elif result[0].find("_OTHERNAME") != -1 and result[2] == gender_2:
                    final_json[result[0] + "_1"] = result[1]
                elif result[0].find("_user") != -1 and result[2] == gender_2:
                    final_json[result[0] + "_2"] = result[1]
                    second_user_json[result[0]] = result[1]
        else:
            # fetching key and value from guide_name_parameters according to language_id and gender of the user for gender 1
            selectionQuery = "SELECT `name_key`, `name_value`, `gender` FROM `guide_name_parameters` WHERE `language_id` = %s AND `gender` = %s"
            # Executing the query
            cursor.execute(selectionQuery, (language_id, gender_1))
            
            for result in cursor:
                if result[0].find("_OTHERNAME") != -1:
                    final_json[result[0] + "_0"] = result[1]
                    final_json[result[0] + "_1"] = result[1]
                elif result[0].find("_user") != -1:
                    final_json[result[0] + "_1"] = result[1]
                    final_json[result[0] + "_2"] = result[1]
                    first_user_json[result[0]] = result[1]
                    second_user_json[result[0]] = result[1]
        
        if rid_1 == rid_2:
            # Fetching the theme and style related to the user
            selectionQuery = "SELECT `rid`, `theme_id` , `style_id` FROM `user_theme_style` WHERE `rid` IN (%s) ORDER BY `theme_id`"
            # Execute the query
            cursor.execute(selectionQuery, (rid_1))
        else:
            # Fetching the theme and style related to the user
            selectionQuery = "SELECT `rid`, `theme_id` , `style_id` FROM `user_theme_style` WHERE `rid` IN (%s, %s) ORDER BY FIELD (`rid`, %s, %s), `theme_id`"
            # Execute the query
            cursor.execute(selectionQuery, (rid_1, rid_2, rid_1, rid_2))
        
        user_theme_list = []
        # fetching the result from cursor
        for result in cursor : user_theme_list.append(result)
        
        logger.info("user_theme_list :::::::::::::::::::::::")
        logger.info(user_theme_list)
        
        user_1_list = []
        user_2_list = []
        
        if rid_1 == rid_2:
            for i in user_theme_list:
                user_1_list.append(str(int(i[1])).zfill(2) + str(int(i[2])).zfill(2))
                user_2_list.append(str(int(i[1])).zfill(2) + str(int(i[2])).zfill(2))
        else:
            for i in user_theme_list:
                if i[0] == rid_1:
                    user_1_list.append(str(int(i[1])).zfill(2) + str(int(i[2])).zfill(2))
                if i[0] == rid_2:
                    user_2_list.append(str(int(i[1])).zfill(2) + str(int(i[2])).zfill(2))
                    
                    
        logger.info("user_1_list ::::::::::::::::::")
        logger.info(user_1_list)
        logger.info("user_2_list ::::::::::::::::::")
        logger.info(user_2_list)
                    
        section_num = []
        product_id = 0
        if report_name == "CouplesGuide":
            product_id = 2
        elif report_name == "CompatibilityGuide":
             product_id = 1
    
        # Fetching the section_ids related to the couples guide
        selectionQuery = "SELECT `section_no` FROM `guide_section_detail` WHERE `product_id`= {}".format(product_id)
        # Execute the query
        cursor.execute(selectionQuery)
        
        # Fetching the section_no's from the cursor
        for result in cursor: section_num.append(int(result[0]))
        
        first_user_json["NAME_user"] = firstname_1
        second_user_json["NAME_user"] = firstname_2
        
        logger.info("first_user_json ::::::::::::::::::::")
        logger.info(first_user_json)
        logger.info("second_user_json ::::::::::::::::::::")
        logger.info(second_user_json)
        logger.info("final_json ::::::::::::::::::::")
        logger.info(final_json)
        
        for i in range(0, len(user_1_list)):
            for j in first_user_json.keys():
                final_json[j.replace("_user","["+str(int(user_1_list[i]))+"]")] = first_user_json[j]
                final_json[j.replace("_user","["+str(int(user_2_list[i]))+"]")] = second_user_json[j]
        
        logger.info("final_json ::::::::::::::::::")
        logger.info(final_json)
        
        
        start_time = time.time()
        # Get Guide Report    
        guides_report = get_guide_report(user_1_list, user_2_list, firstname_1, firstname_2, gender_1, gender_2, report_name, section_num, language_id, cnx, cursor, final_json, product_id)
        
        end_time = time.time()
        logger.info("time for guides_report to run :::::::::::::::::::")
        logger.info(end_time - start_time)
        
        logger.info("guides_report :::::::::::::::::")
        logger.info(guides_report)
        # if product is Couples then executing the below statement
        if auth_token != None and user_partner_id != None and product_id == 2:
         
            # if product_id is 2 and language is english then we have to show coaching
            if language_id==165:
                # showing is_coaching when language is english
                is_coaching = True
            
            # returning success json with the required data
            return {
                        'statusCode': 200,
                        'headers':{
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                                },
                        'body': json.dumps({"picture_url" : picture_url, "partner_firstname":firstname_2,"partner_picture_url":partner_picture_url, "user_partner_id" : user_partner_id,  "is_coaching" : is_coaching, "product_id": product_id, "primary_email":primary_email, "user_name":name, "guides_report" : guides_report})
                    }
        elif  auth_token != None and user_partner_id != None:
            # returning success json with the required data
            return {
                        'statusCode': 200,
                        'headers':{
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                                },
                        'body': json.dumps({"picture_url" : picture_url, "partner_firstname":firstname_2,"partner_picture_url":partner_picture_url, "user_partner_id" : user_partner_id,  "is_coaching" : is_coaching, "product_id": product_id, "primary_email":primary_email, "user_name":name, "guides_report" : guides_report})
                    }
        
        else:
            # returning success json
            return { 
                        'statusCode': 200,
                        'headers' : {
                                        'Access-Control-Allow-Origin': '*',
                                        'Access-Control-Allow-Credentials': 'true'
                                    },
                        'body': json.dumps({"guides_report" : guides_report})
                    }
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
        
if __name__== "__main__":
    handler(None,None)
    