"""
API Module to update contact in active campaign

It provides the following functionalities:
1. log_err(): Logging error and returning the JSON response with error message & status code
2. make_connection(): Connecting to the Database using connection details received through environment variables
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. get_style_code_list() : Returning the user styles code from the Database
5. handler(): Handling the incoming request with following steps:
- Fetching data required for api
- returning the success json with contact id
"""

import json
import logging
import traceback
from os import environ

import jwt
import pymysql
import requests
import configparser

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('activecampaignupdate.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port = environ.get('PORT')
dbuser = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

# tokens and environment variable dependencies
product_romantic_compatability = int(environ.get('product_romantic_compatability'))
product_for_couples = int(environ.get('product_for_couples'))
product_8_date = int(environ.get('product_8_date'))

# dependencies releated active campaign
AC_BASE_URL = environ.get('AC_BASE_URL')
ADD_CONTACT_URL = AC_BASE_URL + environ.get('ADD_CONTACT_URL')
UPDATE_CONTACT_URL = AC_BASE_URL + environ.get('UPDATE_CONTACT_URL')
ADD_CUSTOM_FIELD_VALUE_URL = AC_BASE_URL + environ.get('ADD_CUSTOM_FIELD_VALUE_URL')
ADD_TAG_URL = AC_BASE_URL + environ.get('ADD_TAG_URL')
REMOVE_TAG_URL = AC_BASE_URL + environ.get('REMOVE_TAG_URL')
UPDATE_CUSTOM_FIELD_VALUE_URL = AC_BASE_URL + environ.get('UPDATE_CUSTOM_FIELD_VALUE_URL')
DELETE_CUSTOM_FIELD_VALUE_URL = AC_BASE_URL + environ.get('DELETE_CUSTOM_FIELD_VALUE_URL')
GET_CONTACT_TAG_URL = AC_BASE_URL + environ.get('GET_CONTACT_TAG_URL')
API_TOKEN = environ.get('API_TOKEN')
age_field_id = int(environ.get('AGE_FIELD_ID'))
gender_field_id = int(environ.get('GENDER_FIELD_ID'))
user_id_field_id = int(environ.get('USER_ID_FIELD_ID'))
compatability_guides_field_id = int(environ.get('COMPATABILITY_GUIDES_FIELD_ID'))
couples_guides_field_id = int(environ.get('COUPLES_GUIDES_FIELD_ID'))
language_dict_env = environ.get('LANGUAGE_DICT_ENV')
style_assignment_dict_env = environ.get('STYLE_ASSIGNMENT_DICT_ENV')
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

def get_style_code_list(cursor,rid):
    selectioQuery = "SELECT `theme_style` from `user_theme_style` WHERE rid = %s"
    cursor.execute(selectioQuery, (rid))
    result_list = []
    for result in cursor:
        result = str(result[0])
        if len(result)==3:
            result = "0"+result
        result_list.append(result)
    return result_list

def handler(event,context):
    """Function to handle the request for notifications API"""
    message_by_language = "165_MESSAGES"
    logger.info(event)
    try:
        auth_token = event['headers']['Authorization']
        #style_assignment = "0202"
        #partners_style_assignment = "0202"
    except:
        logger.info(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)

    try:
        # verifying that the user is authorized or not to see this api's data
        rid, user_id, language_id = jwt_verify(auth_token)
        message_by_language = str(language_id) + "_MESSAGES"

    except:
        # if user does not have valid authorization
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['UNAUTHORIZED'], 403)

    try:
        # Making the DB connection
        cnx = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        style_assignment = get_style_code_list(cursor, rid)

        selectionQuery = "SELECT  CAST(AES_DECRYPT(`primary_email`,%s) AS CHAR),`age`,  CAST(AES_DECRYPT(`firstname`,%s) AS CHAR), " \
                         "CAST(AES_DECRYPT(`lastname`,%s) AS CHAR), " \
                         "`gender`, `language_id`, `ac_contact_id` " \
                         "FROM `users` WHERE `id` = %s"

        # Executing the Query
        cursor.execute(selectionQuery, (key, key, key, int(rid)))

        genderFieldValue = ''
        languageContactTag = ''
        styleAssignmentContactTag = ''
        userIdFieldValue = ''
        compatabilityGuidesFieldValue = ''
        couplesGuidesFieldValue = ''
        # product8DatesFieldValue = ''
        # partnersFirstNameFieldValue = ''
        # partnersGenderFieldValue = ''
        # partnersUserIdFieldValue = ''
        # partnerStyleAssignmentContactTag = ''

        result_list = []

        # fetching result from the cursor
        for result in cursor: result_list.append(result)

        # getting the data from the result_list
        email = result_list[0][0]
        age = result_list[0][1]
        first_name = result_list[0][2]
        last_name = result_list[0][3]
        gender = result_list[0][4]
        language = result_list[0][5]
        contact_id = result_list[0][6]
        user_id = user_id
        style_assignment = style_assignment
        #partners_style_assignment = partners_style_assignment

        if gender == 0:
            gender = "Male"
        else:
            gender = "Female"

        selectionQuery = "SELECT COUNT(`product_id`) FROM `user_product` WHERE product_id=%s AND `rid`=%s";
        cursor.execute(selectionQuery, (product_romantic_compatability, int(rid)))
        result_list = []

        # fetching result from the cursor
        for result in cursor: result_list.append(result)
        compatability_guides = result_list[0][0]

        selectionQuery = "SELECT COUNT(`product_id`) FROM `user_product` WHERE product_id=%s AND `rid`=%s";
        cursor.execute(selectionQuery, (product_for_couples, int(rid)))
        result_list = []
        # fetching result from the cursor
        for result in cursor: result_list.append(result)
        couples_guides =  result_list[0][0]


        
        ###################################################################################################################################################
        # Below commented part is regarding product 8 Dates which is not requierd currently. It will though will be used in later release of the project. #
        ###################################################################################################################################################

        


        # selectionQuery = "SELECT COUNT(`product_id`) FROM `user_product` WHERE product_id=%s AND `rid`=%s"
        # cursor.execute(selectionQuery, (product_8_date, int(rid)))
        # result_list = []
        # # fetching result from the cursor

        # for result in cursor: result_list.append(result)

        # product_8_dates = result_list[0][0]

        # if product_8_dates > 0:
        #     is_8_dates = "Yes"
        #     p_8_dates = product_8_dates
        # else:
        #     is_8_dates = "No"
        #     p_8_dates = 0


        genderFieldValue = ''
        ageFieldValue = ''
        languageContactTag = ''
        styleAssignmentContactTag = ''
        userIdFieldValue = ''
        compatabilityGuidesFieldValue = ''
        couplesGuidesFieldValue = ''
        # product8DatesFieldValue = ''
        # partnersFirstNameFieldValue = ''
        # partnersGenderFieldValue = ''
        # partnersUserIdFieldValue = ''
        # partnerStyleAssignmentContactTag = ''



        # if is_8_dates == "Yes":
        #     if event['headers']['user_partner_id'] == None:
        #         selectionQuery = "SELECT `id` FROM `user_product` WHERE `rid`=%s AND `product_id`=%s"
        #         cursor.execute(selectionQuery, (rid, product_8_date))
        #         result_list = []
        #         for result in cursor: result_list.append(result)

        #         selectionQuery = "SELECT `user_partner_id` FROM `user_sub_products` WHERE `user_product_id`=%s"
        #         cursor.execute(selectionQuery, (result_list[-1][0]))
        #         result_list = []
                
        #         for result in cursor: result_list.append(result)
        #         user_partner_id = int(result_list[0][0])

        #         selectionQuery = "SELECT `partner_rid` FROM `user_partner_products` WHERE `id`=%s"
        #         cursor.execute(selectionQuery, (user_partner_id))
        #         result_list = []

        #         # fetching result from the cursor
        #         for result in cursor: result_list.append(result)
        #         partner_rid = result_list[-1][0]

        #         selectionQuery = "SELECT `gender`, cast(AES_DECRYPT(`firstname`, %s) as char), `user_id` FROM `users` WHERE `id` = %s"

        #         # cursor.execute(selectionQuery, (int(partner_rid)))
        #         cursor.execute(selectionQuery, (key,  (int(partner_rid))))

        #         result_list = []
        #         # fetching result from the cursor
        #         for result in cursor: result_list.append(result)
            # else:
        #user_partner_id = int(event['headers']['user_partner_id'])

        #selectionQuery = "SELECT `partner_rid` FROM `user_partner_products` WHERE `id`=%s"
        #cursor.execute(selectionQuery, (user_partner_id))
        #result_list = []

        # fetching result from the cursor
        #for result in cursor: result_list.append(result)
        #partner_rid = result_list[-1][0]

        #selectionQuery = "SELECT `gender`, cast(AES_DECRYPT(`firstname`, %s) as char), `user_id` FROM `users` WHERE `id` = %s"

        # cursor.execute(selectionQuery, (int(partner_rid)))
        #cursor.execute(selectionQuery, (key, (int(partner_rid))))

        #result_list = []
        # fetching result from the cursor

        #for result in cursor: result_list.append(result)


        # if is_8_dates == "Yes":
        #     partners_first_name = result_list[0][1]
        #     partners_gender = result_list[0][0]
        #     partners_user_id = result_list[0][2]
        #     partners_style_assignment = partners_style_assignment
        #     if partners_gender == 0:
        #         partners_gender = "Male"
        #     else:
        #         partners_gender = "Female"
        # else:

            # partners_first_name = ""
            # partners_gender = ""
            # partners_user_id = ""
            # partners_style_assignment = ""


        language_dict = json.loads(language_dict_env)

        style_assignment_dict = json.loads(style_assignment_dict_env)

        languageQuery = "SELECT `name` FROM `language` WHERE id=%s"
        cursor.execute(languageQuery, (int(language)))
        result_list = []
        # fetching result from the cursor

        for result in cursor: result_list.append(result)

        language_id = language_dict[result_list[0][0]]

        # defining a params dict for the parameters to be sent to the API rid
        HEADERS = {'Api-Token': API_TOKEN }

        response = requests.get(url=GET_CONTACT_TAG_URL + '%s' % (contact_id), headers=HEADERS)

        # convert to string.
        contactData = json.dumps(response.json())

        # convert to json.
        contactJsonData = json.loads(contactData)
        # #extract fieldValues element in the contactJsonData
        fieldValues = contactJsonData['fieldValues'][0]

        lengthFieldValues = len(contactJsonData['fieldValues'])
        for i in range(lengthFieldValues):

            if (int(contactJsonData['fieldValues'][i]['field']) == gender_field_id):
                genderFieldValue = contactJsonData['fieldValues'][i]['id']

            elif (int(contactJsonData['fieldValues'][i]['field']) == user_id_field_id):
                userIdFieldValue = contactJsonData['fieldValues'][i]['id']

            elif (int(contactJsonData['fieldValues'][i]['field']) == compatability_guides_field_id ):
                compatabilityGuidesFieldValue = contactJsonData['fieldValues'][i]['id']

            elif (int(contactJsonData['fieldValues'][i]['field']) == couples_guides_field_id):
                couplesGuidesFieldValue = contactJsonData['fieldValues'][i]['id']

            # elif (int(contactJsonData['fieldValues'][i]['field']) == 8):
            #     product8DatesFieldValue = contactJsonData['fieldValues'][i]['id']

            # elif (int(contactJsonData['fieldValues'][i]['field']) == 9):
            #     partnersFirstNameFieldValue = contactJsonData['fieldValues'][i]['id']

            # elif (int(contactJsonData['fieldValues'][i]['field']) == 10):
            #     partnersGenderFieldValue = contactJsonData['fieldValues'][i]['id']

            # elif (int(contactJsonData['fieldValues'][i]['field']) == 11):
            #     partnersUserIdFieldValue = contactJsonData['fieldValues'][i]['id']

            elif (int(contactJsonData['fieldValues'][i]['field']) == age_field_id):
                ageFieldValue = contactJsonData['fieldValues'][i]['id']

        # data to be sent to api
        data = {
            "contact": {
                "email": email,
                "firstName": first_name,
                "lastName": last_name
            }
        }

        data1 = json.dumps(data)
        try:
            try:
                # sending post request and saving response as response object
                response = requests.put(url=UPDATE_CONTACT_URL + '%s' % (contact_id), data=data1, headers=HEADERS)
                # preparing success json  with result_list

                if response.status_code == 422:
                    return {
                        'statusCode': 500,
                        'headers':
                            {
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                            },
                        'body': json.dumps(
                            {"message": "Email address already exists in the system.", "contact_id": contact_id,
                             "fieldValues": {"genderFieldValue": genderFieldValue,
                                             "languageContactTag": languageContactTag,
                                             "styleAssignmentContactTag": styleAssignmentContactTag,
                                             "userIdFieldValue": userIdFieldValue,
                                             "compatabilityGuidesFieldValue": compatabilityGuidesFieldValue,
                                             "couplesGuidesFieldValue": couplesGuidesFieldValue,
                                             # "product8DatesFieldValue": product8DatesFieldValue,
                                             # "partnersFirstNameFieldValue": partnersFirstNameFieldValue,
                                             # "partnersGenderFieldValue": partnersGenderFieldValue,
                                             # "partnersUserIdFieldValue": partnersUserIdFieldValue,
                                             # "partnerStyleAssignmentContactTag": partnerStyleAssignmentContactTag,
                                             "ageFieldValue": ageFieldValue}})
                    }
            except:
                logger.error(traceback.format_exc())
                return log_err("There is some internal problem", 500)


            custom_data_gender_field = {
                "fieldValue": {
                    "contact": contact_id,
                    "field": gender_field_id,
                    "value": gender
                }
            }
            custom_data_gender = json.dumps(custom_data_gender_field)

            response = requests.put(url=UPDATE_CUSTOM_FIELD_VALUE_URL + '%s' % (genderFieldValue), data=custom_data_gender,
                                    headers=HEADERS)
            # preparing success json  with result_list

            # extracting data in json format
            custom_data_gender_field_response = json.dumps(response.json())

            # load the json to a string
            custom_data_gender_field_response_data = json.loads(custom_data_gender_field_response)

            # extract id element in the jsonData
            genderFieldValue = str(custom_data_gender_field_response_data['fieldValue']['id'])

            custom_data_age_field = {
                "fieldValue": {
                    "contact": contact_id,
                    "field": age_field_id,
                    "value": age
                }
            }
            custom_data_age = json.dumps(custom_data_age_field)

            response = requests.put(url=UPDATE_CUSTOM_FIELD_VALUE_URL + '%s' % (ageFieldValue), data=custom_data_age,
                                    headers=HEADERS)
            # preparing success json  with result_list
            # extracting data in json format
            custom_data_age_field_response = json.dumps(response.json())

            # load the json to a string
            custom_data_age_field_response_data = json.loads(custom_data_age_field_response)

            # extract id element in the jsonData
            ageFieldValue = str(custom_data_age_field_response_data['fieldValue']['id'])

            try:
                response = requests.delete(url = REMOVE_TAG_URL+'%s' % (languageContactTag), headers = HEADERS)
            except:
                print("Problem for deleting tag.")

            custom_data_language_tag = {
                "contactTag": {
                    "contact": contact_id,
                    "tag": language_id
                }
            }
            custom_data_language = json.dumps(custom_data_language_tag)

            response = requests.post(url=ADD_TAG_URL, data=custom_data_language, headers=HEADERS)
            # preparing success json  with result_list

            # extracting data in json format
            custom_data_language_response = json.dumps(response.json())

            # load the json to a string
            custom_data_language_response_data = json.loads(custom_data_language_response)
            # extract id element in the jsonData
            languageContactTag = str(custom_data_language_response_data['contactTag']['id'])

            try:
                styleAssignmentTagIdForDelete = styleAssignmentContactTag.split(",")
                for tagId in styleAssignmentTagIdForDelete:
                    response = requests.delete(url = REMOVE_TAG_URL+'%s' % (tagId), headers = HEADERS)
                    styleAssignmentContactTag = ''
            except:
                print("Problem for deleting tag.")

            styleAssignment = style_assignment
            for style_assignment_value in styleAssignment:
                style_assignment_id = style_assignment_dict[style_assignment_value]
                custom_data_style_assignment_tag = {
                    "contactTag": {
                        "contact": contact_id,
                        "tag": style_assignment_id
                    }
                }
                custom_data_style_assignment = json.dumps(custom_data_style_assignment_tag)


                response = requests.post(url=ADD_TAG_URL, data=custom_data_style_assignment, headers=HEADERS)
                # preparing success json  with result_list

                # extracting data in json format
                custom_data_style_assignment_response = json.dumps(response.json())

                # load the json to a string
                custom_data_style_assignment_response_data = json.loads(custom_data_style_assignment_response)
                # extract id element in the jsonData
                styleAssignmentContactTagId = str(custom_data_style_assignment_response_data['contactTag']['id'])
                styleAssignmentContactTag = styleAssignmentContactTag + "," + styleAssignmentContactTagId

            styleAssignmentContactTag = styleAssignmentContactTag[1:]

            custom_data_user_id_field = {
                "fieldValue": {
                    "contact": contact_id,
                    "field": user_id_field_id,
                    "value": user_id
                }
            }
            custom_data_user_id = json.dumps(custom_data_user_id_field)

            response = requests.put(url=UPDATE_CUSTOM_FIELD_VALUE_URL + '%s' % (userIdFieldValue), data=custom_data_user_id,
                                    headers=HEADERS)
            # preparing success json  with result_list
            # extracting data in json format
            custom_data_user_id_field_response = json.dumps(response.json())

            # load the json to a string
            custom_data_user_id_field_response_data = json.loads(custom_data_user_id_field_response)

            # extract id element in the jsonData
            userIdFieldValue = str(custom_data_user_id_field_response_data['fieldValue']['id'])

            custom_data_compatability_guides_field = {
                "fieldValue": {
                    "contact": contact_id,
                    "field": compatability_guides_field_id,
                    "value": compatability_guides
                }
            }
            custom_data_compatability_guides = json.dumps(custom_data_compatability_guides_field)

            response = requests.put(url=UPDATE_CUSTOM_FIELD_VALUE_URL + '%s' % (compatabilityGuidesFieldValue),
                                    data=custom_data_compatability_guides, headers=HEADERS)
            # preparing success json  with result_list
            # extracting data in json format
            custom_data_compatability_guides_field_response = json.dumps(response.json())

            # load the json to a string
            custom_data_compatability_guides_field_response_data = json.loads(
                custom_data_compatability_guides_field_response)

            # extract id element in the jsonData
            compatabilityGuidesFieldValue = str(custom_data_compatability_guides_field_response_data['fieldValue']['id'])

            custom_data_couples_guides_field = {
                "fieldValue": {
                    "contact": contact_id,
                    "field": couples_guides_field_id,
                    "value": couples_guides
                }
            }

            custom_data_couples_guides = json.dumps(custom_data_couples_guides_field)

            response = requests.put(url=UPDATE_CUSTOM_FIELD_VALUE_URL + '%s' % (couplesGuidesFieldValue),
                                    data=custom_data_couples_guides, headers=HEADERS)
            # preparing success json  with result_list
            # extracting data in json format
            custom_data_couples_guides_field_response = json.dumps(response.json())

            # load the json to a string
            custom_data_couples_guides_field_response_data = json.loads(custom_data_couples_guides_field_response)

            # extract id element in the jsonData
            couplesGuidesFieldValue = str(custom_data_couples_guides_field_response_data['fieldValue']['id'])

            # custom_data_8_dates_field = {
            #     "fieldValue": {
            #         "contact": contact_id,
            #         "field": 8,
            #         "value": product_8_dates
            #     }
            # }
            # custom_data_8_dates = json.dumps(custom_data_8_dates_field)

            # response = requests.put(url=UPDATE_CUSTOM_FIELD_VALUE_URL + '%s' % (product8DatesFieldValue),
            #                         data=custom_data_8_dates, headers=HEADERS)
            # # preparing success json  with result_list
            # # extracting data in json format
            # custom_data_8_dates_field_response = json.dumps(response.json())

            # # load the json to a string
            # custom_data_8_dates_field_response_data = json.loads(custom_data_8_dates_field_response)

            # # extract id element in the jsonData
            # product8DatesFieldValue = str(custom_data_8_dates_field_response_data['fieldValue']['id'])
            # if is_8_dates == "Yes":

            #     if (partnersFirstNameFieldValue == ''):
            #         partners_first_name_field = {
            #             "fieldValue": {
            #                 "contact": contact_id,
            #                 "field": 9,
            #                 "value": partners_first_name
            #             }
            #         }
            #         partners_first_name = json.dumps(partners_first_name_field)

            #         response = requests.post(url=ADD_CUSTOM_FIELD_VALUE_URL, data=partners_first_name, headers=HEADERS)
            #         # preparing success json  with result_list
            #         # extracting data in json format
            #         partners_first_name_field_response = json.dumps(response.json())

            #         # load the json to a string
            #         partners_first_name_field_response_data = json.loads(partners_first_name_field_response)

            #         # extract id element in the jsonData
            #         partnersFirstNameFieldValue = str(partners_first_name_field_response_data['fieldValue']['id'])



                # else:
                #     partners_first_name_field = {
                #         "fieldValue": {
                #             "contact": contact_id,
                #             "field": 9,
                #             "value": partners_first_name
                #         }
                #     }
                #     partners_first_name = json.dumps(partners_first_name_field)

                #     response = requests.put(url=UPDATE_CUSTOM_FIELD_VALUE_URL + '%s' % (partnersFirstNameFieldValue),
                #                             data=partners_first_name, headers=HEADERS)
                #     # preparing success json  with result_list
                #     # extracting data in json format
                #     partners_first_name_field_response = json.dumps(response.json())

                #     # load the json to a string
                #     partners_first_name_field_response_data = json.loads(partners_first_name_field_response)

                #     # extract id element in the jsonData
                #     partnersFirstNameFieldValue = str(partners_first_name_field_response_data['fieldValue']['id'])



                # if (partnersGenderFieldValue == ''):
                #     partners_gender_field = {
                #         "fieldValue": {
                #             "contact": contact_id,
                #             "field": 10,
                #             "value": str(partners_gender).capitalize()
                #         }
                #     }
                #     partners_gender = json.dumps(partners_gender_field)

                #     response = requests.post(url=ADD_CUSTOM_FIELD_VALUE_URL, data=partners_gender, headers=HEADERS)
                #     # preparing success json  with result_list
                #     # extracting data in json format
                #     partners_gender_field_response = json.dumps(response.json())
                    
                #     logger.info(response)

                #     # load the json to a string
                #     partners_gender_field_response_data = json.loads(partners_gender_field_response)

                #     # extract id element in the jsonData
                #     partnersGenderFieldValue = str(partners_gender_field_response_data['fieldValue']['id'])

                # else:

                #     partners_gender_field = {
                #         "fieldValue": {
                #             "contact": contact_id,
                #             "field": 10,
                #             "value": str(partners_gender).capitalize()
                #         }
                #     }
                #     partners_gender = json.dumps(partners_gender_field)

                #     response = requests.put(url=UPDATE_CUSTOM_FIELD_VALUE_URL + '%s' % (partnersGenderFieldValue),
                #                             data=partners_gender, headers=HEADERS)
                #     # preparing success json  with result_list
                #     # extracting data in json format
                #     partners_gender_field_response = json.dumps(response.json())

                #     # load the json to a string
                #     partners_gender_field_response_data = json.loads(partners_gender_field_response)

                #     # extract id element in the jsonData
                #     partnersGenderFieldValue = str(partners_gender_field_response_data['fieldValue']['id'])

                # if (partnersUserIdFieldValue == ''):
                #     partners_user_id_field = {
                #         "fieldValue": {
                #             "contact": contact_id,
                #             "field": 11,
                #             "value": partners_user_id
                #         }
                #     }
                #     partners_user_id = json.dumps(partners_user_id_field)

                #     response = requests.post(url=ADD_CUSTOM_FIELD_VALUE_URL, data=partners_user_id, headers=HEADERS)
                #     # preparing success json  with result_list
                #     # extracting data in json format
                #     partners_user_id_field_response = json.dumps(response.json())

                #     # load the json to a string
                #     partners_user_id_field_response_data = json.loads(partners_user_id_field_response)

                #     # extract id element in the jsonData
                #     partnersUserIdFieldValue = str(partners_user_id_field_response_data['fieldValue']['id'])

                # else:
                #     partners_user_id_field = {
                #         "fieldValue": {
                #             "contact": contact_id,
                #             "field": 11,
                #             "value": partners_user_id
                #         }
                #     }
                #     partners_user_id = json.dumps(partners_user_id_field)

                #     response = requests.put(url=UPDATE_CUSTOM_FIELD_VALUE_URL + '%s' % (partnersUserIdFieldValue),
                #                             data=partners_user_id, headers=HEADERS)
                #     # preparing success json  with result_list
                #     # extracting data in json format
                #     partners_user_id_field_response = json.dumps(response.json())

                #     # load the json to a string
                #     partners_user_id_field_response_data = json.loads(partners_user_id_field_response)

                #     # extract id element in the jsonData
                #     partnersUserIdFieldValue = str(partners_user_id_field_response_data['fieldValue']['id'])

                # if (partnerStyleAssignmentContactTag == ''):
                #     partnerStyleAssignment = partners_style_assignment.split(",")
                #     for partner_style_assignment_value in partnerStyleAssignment:
                #         partner_style_assignment_id = style_assignment_dict[partner_style_assignment_value]
                #         custom_data_partner_style_assignment_tag = {
                #             "contactTag": {
                #                 "contact": contact_id,
                #                 "tag": partner_style_assignment_id
                #             }
                #         }
                #         custom_data_partner_style_assignment = json.dumps(custom_data_partner_style_assignment_tag)

                #         response = requests.post(url=ADD_TAG_URL, data=custom_data_partner_style_assignment, headers=HEADERS)
                #         # preparing success json  with result_list
                #         # extracting data in json format
                #         custom_data_partner_style_assignment_response = json.dumps(response.json())

                #         # load the json to a string
                #         custom_data_partner_style_assignment_response_data = json.loads(
                #             custom_data_partner_style_assignment_response)

                #         # extract id element in the jsonData
                #         partnerStyleAssignmentContactTagId = str(
                #             custom_data_partner_style_assignment_response_data['contactTag']['id'])

                #         partnerStyleAssignmentContactTag = partnerStyleAssignmentContactTag + "," + partnerStyleAssignmentContactTagId
                #     partnerStyleAssignmentContactTag = partnerStyleAssignmentContactTag[1:]
                # else:

                #     try:
                #         partnerStyleAssignmentTagIdForDelete = partnerStyleAssignmentContactTag.split(",")
                #         for tagId in partnerStyleAssignmentTagIdForDelete:
                #             response = requests.delete(url=REMOVE_TAG_URL + '%s' % (tagId), headers=HEADERS)
                #             # response = requests.request("DELETE", DELETE_TAG_URL)
                #             partnerStyleAssignmentContactTag = ''
                #     except:

                #     partnerStyleAssignment = partners_style_assignment.split(",")
                #     for partner_style_assignment_value in partnerStyleAssignment:
                #         partner_style_assignment_id = style_assignment_dict[partner_style_assignment_value]
                #         custom_data_partner_style_assignment_tag = {
                #             "contactTag": {
                #                 "contact": contact_id,
                #                 "tag": partner_style_assignment_id
                #             }
                #         }
                #         custom_data_partner_style_assignment = json.dumps(custom_data_partner_style_assignment_tag)

                #         response = requests.post(url=ADD_TAG_URL, data=custom_data_partner_style_assignment, headers=HEADERS)
                #         # preparing success json  with result_list
                #         # extracting data in json format
                #         custom_data_partner_style_assignment_response = json.dumps(response.json())

                #         # load the json to a string
                #         custom_data_partner_style_assignment_response_data = json.loads(
                #             custom_data_partner_style_assignment_response)

                #         # extract id element in the jsonData
                #         partnerStyleAssignmentContactTagId = str(
                #             custom_data_partner_style_assignment_response_data['contactTag']['id'])

                #         partnerStyleAssignmentContactTag = partnerStyleAssignmentContactTag + "," + partnerStyleAssignmentContactTagId
                #     partnerStyleAssignmentContactTag = partnerStyleAssignmentContactTag[1:]

            # elif is_8_dates == "No":

            #     if (partnersFirstNameFieldValue != ''):
            #         response = requests.delete(url=DELETE_CUSTOM_FIELD_VALUE_URL + '%s' % (partnersFirstNameFieldValue),headers=HEADERS)
            #         # preparing success json  with result_list
            #         partnersFirstNameFieldValue = ''

            #     if (partnersGenderFieldValue != ''):
            #         response = requests.delete(url=DELETE_CUSTOM_FIELD_VALUE_URL + '%s' % (partnersGenderFieldValue),headers=HEADERS)
            #         # preparing success json  with result_list
            #         partnersGenderFieldValue = ''

            #     if (partnersUserIdFieldValue != ''):
            #         response = requests.delete(url=DELETE_CUSTOM_FIELD_VALUE_URL + '%s' % (partnersUserIdFieldValue),headers=HEADERS)
            #         # preparing success json  with result_list
            #         partnersUserIdFieldValue = ''

            #     if (partnerStyleAssignmentContactTag != ''):
            #         try:
            #             partnerStyleAssignmentTagIdForDelete = partnerStyleAssignmentContactTag.split(",")
            #             for tagId in partnerStyleAssignmentTagIdForDelete:
            #                 response = requests.delete(url=REMOVE_TAG_URL + '%s' % (tagId), headers=HEADERS)
            #                 partnerStyleAssignmentContactTag = ''
            #         except:

            finalJson = json.dumps({"message": "Contact Details updated successfully.", "contact_id": contact_id,
                                    "fieldValues": {"genderFieldValue": genderFieldValue,
                                                    "languageContactTag": languageContactTag,
                                                    "styleAssignmentContactTag": styleAssignmentContactTag,
                                                    "userIdFieldValue": userIdFieldValue,
                                                    "compatabilityGuidesFieldValue": compatabilityGuidesFieldValue,
                                                    "couplesGuidesFieldValue": couplesGuidesFieldValue,
                                                    # "product8DatesFieldValue": product8DatesFieldValue,
                                                    # "partnersFirstNameFieldValue": partnersFirstNameFieldValue,
                                                    # "partnersGenderFieldValue": partnersGenderFieldValue,
                                                    # "partnersUserIdFieldValue": partnersUserIdFieldValue,
                                                    # "partnerStyleAssignmentContactTag": partnerStyleAssignmentContactTag,
                                                    "ageFieldValue": ageFieldValue}})

            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
                'body': json.dumps({"message": "Contact Details updated successfully.", "contact_id": contact_id,
                                    "fieldValues": {"genderFieldValue": genderFieldValue,
                                                    "languageContactTag": languageContactTag,
                                                    "styleAssignmentContactTag": styleAssignmentContactTag,
                                                    "userIdFieldValue": userIdFieldValue,
                                                    "compatabilityGuidesFieldValue": compatabilityGuidesFieldValue,
                                                    "couplesGuidesFieldValue": couplesGuidesFieldValue,
                                                    # "product8DatesFieldValue": product8DatesFieldValue,
                                                    # "partnersFirstNameFieldValue": partnersFirstNameFieldValue,
                                                    # "partnersGenderFieldValue": partnersGenderFieldValue,
                                                    # "partnersUserIdFieldValue": partnersUserIdFieldValue,
                                                    # "partnerStyleAssignmentContactTag": partnerStyleAssignmentContactTag,
                                                    "ageFieldValue": ageFieldValue}})
            }
        except:
            logger.error(traceback.format_exc())
            return log_err('Data request sent in wrong format', 500)

    except:
        logger.info(traceback.format_exc())
        # If there is any error in above operations, logging the error
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)

    finally:
        try:
            # Finally, clean up the connection
            cursor.close()
            cnx.close()
        except:
            pass        
if __name__== "__main__":
    handler(None, None)
