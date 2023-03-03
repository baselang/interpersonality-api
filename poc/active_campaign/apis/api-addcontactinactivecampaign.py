"""
API Module to add contact in active campaign

It provides the following functionalities:
1. log_err(): Logging error and returning the JSON response with error message & status code
2. handler(): Handling the incoming request with following steps:
- Fetching data required for api
- returning the success json with contact Id

"""

import json
import logging
import traceback
import requests
from os import environ
import configparser
from datetime import datetime

# # reading values from property file to get all the response messages
# config = configparser.ConfigParser()
# config.read('addcontactinactivecampaign.properties')

GENDER_FIELD_ID = environ.get('GENDER_FIELD_ID')  #field Id : 1
#LANGUAGE_FIELD_ID = environ.get('LANGUAGE_FIELD_ID') #field Id : 2
#ALL_OF_THEIR_OWN_STYLE_ASSIGNMENTS_TAGS_ID = environ.get('ALL_OF_THEIR_OWN_STYLE_ASSIGNMENTS_TAGS_ID') #field Id : 3
USER_ID_FIELD_ID = environ.get('USER_ID_FIELD_ID') #field Id : 4
NO_OF_COMPATABILITY_GUIDES_FIELD_ID = environ.get('NO_OF_COMPATABILITY_GUIDES_FIELD_ID') #field Id : 5
NO_OF_FOR_COUPLES_GUIDES_FIELD_ID = environ.get('NO_OF_FOR_COUPLES_GUIDES_FIELD_ID') #field Id : 6
NO_OF_8_DATES_FIELD_ID = environ.get('NO_OF_8_DATES_FIELD_ID') #field Id : 7
PARTNERS_FIRST_NAME_FIELD_ID = environ.get('PARTNERS_FIRST_NAME_FIELD_ID')
PARTNERS_GENDER_FIELD_ID = environ.get('PARTNERS_GENDER_FIELD_ID')
PARTNERS_USER_ID_FIELD_ID = environ.get('PARTNERS_USER_ID_FIELD_ID')
#ALL_OF_THEIR_PARTNERS_STYLES_TAGS_ID = environ.get('ALL_OF_THEIR_PARTNERS_STYLES_TAGS_ID')
ADD_CONTACT_URL = environ.get('ADD_CONTACT_URL') #https://certaintyinfotech.api-us1.com/api/3/contact/sync
ADD_CUSTOM_FIELD_VALUE_URL = environ.get('ADD_CUSTOM_FIELD_VALUE_URL') #https://certaintyinfotech.api-us1.com/api/3/fieldValues
ADD_TAG_URL = environ.get('ADD_CUSTOM_FIELD_VALUE_URL') #https://certaintyinfotech.api-us1.com/api/3/contactTags
API_TOKEN = environ.get('API_TOKEN') #213988f23b751601cb3161d22386c73ce2a45c0437d5802ca0fcd23cf0ba86f49a636ac8


# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
# Setting the log level to INFO
logger.setLevel(logging.INFO)

logger.info("Cold start complete.")


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
    """Function to handle the request for notifications API"""
    message_by_language = "165_MESSAGES"
    logger.info(event)
    data = json.loads(event['body'])
    email = data["email"]
    age = data["age"]
    first_name = data["first_name"]
    last_name = data["last_name"]
    gender = data["gender"]
    language = data["language"]
    style_assignment = data["style_assignment"]
    user_id = data["user_id"]
    compatability_guides = data["compatability_guides"]
    couples_guides = data["couples_guides"]
    product_8_dates = data["product_8_dates"]
    is_8_dates = data["is_8_dates"]

    genderFieldValue = ''
    ageFieldValue = ''
    languageContactTag = ''
    styleAssignmentContactTag = ''
    userIdFieldValue = ''
    compatabilityGuidesFieldValue = ''
    couplesGuidesFieldValue = ''
    product8DatesFieldValue = ''
    partnersFirstNameFieldValue = ''
    partnersGenderFieldValue = ''
    partnersUserIdFieldValue = ''
    partnerStyleAssignmentContactTag = ''

    if(is_8_dates == "Yes"):
        partners_first_name = data["partners_first_name"]
        partners_gender = data["partners_gender"]
        partners_user_id = data["partners_user_id"]
        partners_style_assignment = data["partner_style_assignment"]
  
    language_dict = {"English": 6, "Spanish": 7}
    #style_assignment_dict = {"0101": 8, "0102": 9,"0103": 10,"0104": 11,"0105": 12,"0106": 13,"0201": 14, "0202": 15,"0203": 16,"0301": 17, "0302": 18,"0401": 19, "0402": 20,"0403": 21,"0501": 22, "0502": 23,"0503": 24,"0601": 25}
    style_assignment_dict = {"0101": 8, "0102": 9,"0103": 10,"0104": 11,"0105": 12,"0106": 13,"0201": 14, "0202": 15,"0203": 16,"0301": 17, "0302": 18,"0401": 19, "0402": 20,"0403": 21,"0501": 22, "0502": 23,"0503": 24,"0601": 25,"0602": 26,"0701": 27, "0702": 28,"0801": 29, "0802": 30,"0803": 31,"0804": 32,"0901": 33, "0902": 34,"1001": 35, "1002": 36,"1003": 37,"1004": 38,"1005": 39,"1101": 40, "1102": 41,"1103": 42,"1201": 43, "1202": 44,"1203": 45,"1301": 46, "1302": 47,"1303": 48,"1304": 49, "1305": 50,"1306": 51,"1307": 52, "1308": 53,"1401": 54, "1402": 55,"1403": 56,"1404": 57, "1405": 58,"1406": 59,"1407": 60, "1408": 61}


    language_id = language_dict[language]
    

    # if(is_8_dates == "Yes"):       
    #     print ('partners_first_name:', partners_first_name)
    #     print ('partners_gender:', partners_gender)
    #     print ('partners_user_id:', partners_user_id)
    #     print ('partners_style_assignment:', partners_style_assignment)

    # print ('language_id:', language_id)
    # print ('age:', age)
    # print ('product_8_dates:', product_8_dates)
    # print ('first_name:', first_name)
    # print ('last_name:', last_name)
    # print ('language:', language)
    # print ('gender:', gender)
    # print ('user_id:', user_id)
    # print ('style_assignment:', style_assignment)
    # print ('compatability_guides:', compatability_guides)
    # print ('couples_guides:', couples_guides)
    # print ('product_8_dates:', product_8_dates)
    
    # defining a params dict for the parameters to be sent to the API rid
    HEADERS = { 'Api-Token':'213988f23b751601cb3161d22386c73ce2a45c0437d5802ca0fcd23cf0ba86f49a636ac8'
    } 

    ADD_CONTACT_URL = 'https://certaintyinfotech.api-us1.com/api/3/contact/sync'
    ADD_CUSTOM_FIELD_VALUE_URL = 'https://certaintyinfotech.api-us1.com/api/3/fieldValues'
    ADD_TAG_URL = 'https://certaintyinfotech.api-us1.com/api/3/contactTags'
    # data to be sent to api 
    data ={
    "contact": {
        "email": email,
        "firstName": first_name,
        "lastName": last_name
    }
}


    data1 = json.dumps(data)

    print("data1:",data1)

    try:
        # sending post request and saving response as response object 
        response = requests.post(url = ADD_CONTACT_URL, data = data1, headers = HEADERS)
        data = json.dumps(response.json())
        print ('data:', data)

        #load the json to a string
        jsonData = json.loads(data)
        
        #extract contact_id element in the jsonData
        contact_id = str(jsonData['contact']['id'])
        print ('contact_id:', contact_id)


        custom_data_age_field ={
            "fieldValue": {
            "contact": contact_id,
            "field": 21,
            "value": age
            }
        }
        custom_data_age = json.dumps(custom_data_age_field)
        #print("custom_data_male :",custom_data_male)

        response = requests.post(url = ADD_CUSTOM_FIELD_VALUE_URL, data = custom_data_age, headers = HEADERS)
        # preparing success json  with result_list
        print("custom_data_age_field_response :",response)
        # extracting data in json format 
        custom_data_age_field_response = json.dumps(response.json())
        #print ('custom_data_gender_field_response:', custom_data_gender_field_response)

        #load the json to a string
        custom_data_age_field_response_data = json.loads(custom_data_age_field_response)
        #print ('jsonData:', jsonData)


        #extract id element in the jsonData
        ageFieldValue = str(custom_data_age_field_response_data['fieldValue']['id'])
        print ('ageFieldValue:', ageFieldValue)


        custom_data_gender_field ={
            "fieldValue": {
            "contact": contact_id,
            "field": 20,
            "value": gender
            }
        }
        custom_data_gender = json.dumps(custom_data_gender_field)
        #print("custom_data_male :",custom_data_male)

        response = requests.post(url = ADD_CUSTOM_FIELD_VALUE_URL, data = custom_data_gender, headers = HEADERS)
        # preparing success json  with result_list
        print("custom_data_gender_field_response :",response)
        # extracting data in json format 
        custom_data_gender_field_response = json.dumps(response.json())
        #print ('custom_data_gender_field_response:', custom_data_gender_field_response)

        #load the json to a string
        custom_data_gender_field_response_data = json.loads(custom_data_gender_field_response)
        #print ('jsonData:', jsonData)


        #extract id element in the jsonData
        genderFieldValue = str(custom_data_gender_field_response_data['fieldValue']['id'])
        print ('genderFieldValue:', genderFieldValue)


        custom_data_language_tag ={
            "contactTag": {
            "contact": contact_id,
            "tag": language_id
            }
        }
        custom_data_language = json.dumps(custom_data_language_tag)
        print("custom_data_language:",custom_data_language)

        response = requests.post(url = ADD_TAG_URL, data = custom_data_language, headers = HEADERS)
        # preparing success json  with result_list
        print("custom_data_language_response :",response)
        # extracting data in json format 
        custom_data_language_response = json.dumps(response.json())
        #print ('custom_data_language_response:', custom_data_language_response)


        #load the json to a string
        custom_data_language_response_data = json.loads(custom_data_language_response)
        #print ('jsonData:', jsonData)


        #extract id element in the jsonData
        languageContactTag = str(custom_data_language_response_data['contactTag']['id'])
        print ('languageContactTag:', languageContactTag)

        styleAssignment = style_assignment.split(",")
        print("styleAssignment :",styleAssignment) 
        for style_assignment_value in styleAssignment:
            style_assignment_id = style_assignment_dict[style_assignment_value]
            custom_data_style_assignment_tag ={
                "contactTag": {
                "contact": contact_id,
                "tag": style_assignment_id
                }
            }
            custom_data_style_assignment = json.dumps(custom_data_style_assignment_tag)
            print("custom_data_style_assignment:",custom_data_style_assignment)
    
            response = requests.post(url = ADD_TAG_URL, data = custom_data_style_assignment, headers = HEADERS)
            # preparing success json  with result_list
            print("custom_data_style_assignment_response :",response)
            # extracting data in json format 
            custom_data_style_assignment_response = json.dumps(response.json())
            #print ('custom_data_style_assignment_response:', custom_data_style_assignment_response)
    
            #load the json to a string
            custom_data_style_assignment_response_data = json.loads(custom_data_style_assignment_response)
            #print ('jsonData:', jsonData)
    
    
            #extract id element in the jsonData
            styleAssignmentContactTagId = str(custom_data_style_assignment_response_data['contactTag']['id'])
            #print ('styleAssignmentContactTag:', styleAssignmentContactTag)
            styleAssignmentContactTag = styleAssignmentContactTag  +","+ styleAssignmentContactTagId

        styleAssignmentContactTag = styleAssignmentContactTag[1:]
        

        custom_data_user_id_field ={
            "fieldValue": {
            "contact": contact_id,
            "field": 4,
            "value": user_id
            }
        }
        custom_data_user_id = json.dumps(custom_data_user_id_field)
        #print("custom_data_user_id:",custom_data_user_id)

        response = requests.post(url = ADD_CUSTOM_FIELD_VALUE_URL, data = custom_data_user_id, headers = HEADERS)
        # preparing success json  with result_list
        print("custom_data_user_id_field_response :",response)
        # extracting data in json format 
        custom_data_user_id_field_response = json.dumps(response.json())
        #print ('custom_data_user_id_field_response:', custom_data_user_id_field_response)

        #load the json to a string
        custom_data_user_id_field_response_data = json.loads(custom_data_user_id_field_response)
        #print ('jsonData:', jsonData)


        #extract id element in the jsonData
        userIdFieldValue = str(custom_data_user_id_field_response_data['fieldValue']['id'])
        print ('userIdFieldValue:', userIdFieldValue)



        custom_data_compatability_guides_field ={
            "fieldValue": {
            "contact": contact_id,
            "field": 5,
            "value": compatability_guides
            }
        }
        custom_data_compatability_guides = json.dumps(custom_data_compatability_guides_field)
        #print("data4 100:",custom_data_compatability_guides)

        response = requests.post(url = ADD_CUSTOM_FIELD_VALUE_URL, data = custom_data_compatability_guides, headers = HEADERS)
        # preparing success json  with result_list
        print("custom_data_compatability_guides_field_response :",response)
        # extracting data in json format 
        custom_data_compatability_guides_field_response = json.dumps(response.json())
        #print ('custom_data_compatability_guides_field_response:', custom_data_compatability_guides_field_response)

        #load the json to a string
        custom_data_compatability_guides_field_response_data = json.loads(custom_data_compatability_guides_field_response)
        #print ('jsonData:', jsonData)


        #extract id element in the jsonData
        compatabilityGuidesFieldValue = str(custom_data_compatability_guides_field_response_data['fieldValue']['id'])
        print ('compatabilityGuidesFieldValue:', compatabilityGuidesFieldValue)


        custom_data_couples_guides_field ={
            "fieldValue": {
            "contact": contact_id,
            "field": 6,
            "value": couples_guides
            }
        }

        custom_data_couples_guides = json.dumps(custom_data_couples_guides_field)
        #print("custom_data_couples_guides:",custom_data_couples_guides)

        response = requests.post(url = ADD_CUSTOM_FIELD_VALUE_URL, data = custom_data_couples_guides, headers = HEADERS)
        # preparing success json  with result_list
        print("custom_data_couples_guides_field_response :",response)
        # extracting data in json format 
        custom_data_couples_guides_field_response = json.dumps(response.json())
        #print ('custom_data_couples_guides_field_response:', custom_data_couples_guides_field_response)

        #load the json to a string
        custom_data_couples_guides_field_response_data = json.loads(custom_data_couples_guides_field_response)
        #print ('jsonData:', jsonData)


        #extract id element in the jsonData
        couplesGuidesFieldValue = str(custom_data_couples_guides_field_response_data['fieldValue']['id'])
        print ('couplesGuidesFieldValue:', couplesGuidesFieldValue)

        custom_data_8_dates_field ={
            "fieldValue": {
            "contact": contact_id,
            "field": 7,
            "value": product_8_dates
            }
        }
        custom_data_8_dates = json.dumps(custom_data_8_dates_field)
        #print("custom_data_8_dates:",custom_data_8_dates)

        response = requests.post(url = ADD_CUSTOM_FIELD_VALUE_URL, data = custom_data_8_dates, headers = HEADERS)
        # preparing success json  with result_list
        print("custom_data_8_dates_field_response :",response)
        # extracting data in json format 
        custom_data_8_dates_field_response = json.dumps(response.json())
        #print ('custom_data_8_dates_field_response:', custom_data_8_dates_field_response)

        #load the json to a string
        custom_data_8_dates_field_response_data = json.loads(custom_data_8_dates_field_response)
        #print ('jsonData:', jsonData)


        #extract id element in the jsonData
        product8DatesFieldValue = str(custom_data_8_dates_field_response_data['fieldValue']['id'])
        print ('product8DatesFieldValue:', product8DatesFieldValue)

        if(is_8_dates == "Yes"):
            
            partners_first_name_field ={
                "fieldValue": {
                "contact": contact_id,
                "field": 16,
                "value": partners_first_name
                }
            }
            partners_first_name = json.dumps(partners_first_name_field)
            #print("partners_first_name:",partners_first_name)

            response = requests.post(url = ADD_CUSTOM_FIELD_VALUE_URL, data = partners_first_name, headers = HEADERS)
            # preparing success json  with result_list
            print("partners_first_name_field_response :",response)
            # extracting data in json format 
            partners_first_name_field_response = json.dumps(response.json())
            #print ('partners_first_name_field_response:', partners_first_name_field_response)

            #load the json to a string
            partners_first_name_field_response_data = json.loads(partners_first_name_field_response)
            #print ('jsonData:', jsonData)


            #extract id element in the jsonData
            partnersFirstNameFieldValue = str(partners_first_name_field_response_data['fieldValue']['id'])
            print ('partnersFirstNameFieldValue:', partnersFirstNameFieldValue)


            partners_gender_field ={
                "fieldValue": {
                "contact": contact_id,
                "field": 17,
                "value": partners_gender
                }
            }
            partners_gender = json.dumps(partners_gender_field)
            #print("partners_first_name:",partners_gender)

            response = requests.post(url = ADD_CUSTOM_FIELD_VALUE_URL, data = partners_gender, headers = HEADERS)
            # preparing success json  with result_list
            print("partners_gender_field_response :",response)
            # extracting data in json format 
            partners_gender_field_response = json.dumps(response.json())
            #print ('partners_gender_field_response:', partners_gender_field_response)


            #load the json to a string
            partners_gender_field_response_data = json.loads(partners_gender_field_response)
            #print ('jsonData:', jsonData)


            #extract id element in the jsonData
            partnersGenderFieldValue = str(partners_gender_field_response_data['fieldValue']['id'])
            print ('partnersGenderFieldValue:', partnersGenderFieldValue)



            partners_user_id_field ={
                    "fieldValue": {
                    "contact": contact_id,
                    "field": 18,
                    "value": partners_user_id
                    }
                }
            partners_user_id = json.dumps(partners_user_id_field)
            #print("partners_user_id:",partners_user_id)

            response = requests.post(url = ADD_CUSTOM_FIELD_VALUE_URL, data = partners_user_id, headers = HEADERS)
            # preparing success json  with result_list
            print("partners_user_id_field_response :",response)
            # extracting data in json format 
            partners_user_id_field_response = json.dumps(response.json())
            #print ('partners_user_id_field_response:', partners_user_id_field_response)


            #load the json to a string
            partners_user_id_field_response_data = json.loads(partners_user_id_field_response)
            #print ('jsonData:', jsonData)


            #extract id element in the jsonData
            partnersUserIdFieldValue = str(partners_user_id_field_response_data['fieldValue']['id'])
            print ('partnersUserIdFieldValue:', partnersUserIdFieldValue)

            partnerStyleAssignment = partners_style_assignment.split(",")
            print("partnerStyleAssignment :",partnerStyleAssignment) 
            for partner_style_assignment_value in partnerStyleAssignment:
                partner_style_assignment_id = style_assignment_dict[partner_style_assignment_value]
                custom_data_partner_style_assignment_tag ={
                    "contactTag": {
                    "contact": contact_id,
                    "tag": partner_style_assignment_id
                    }
                }
                custom_data_partner_style_assignment = json.dumps(custom_data_partner_style_assignment_tag)
                print("custom_data_partner_style_assignment:",custom_data_partner_style_assignment)
    
                response = requests.post(url = ADD_TAG_URL, data = custom_data_partner_style_assignment, headers = HEADERS)
                # preparing success json  with result_list
                print("custom_data_partner_style_assignment_response :",response)
                # extracting data in json format 
                custom_data_partner_style_assignment_response = json.dumps(response.json())
                #print ('custom_data_partner_style_assignment_response:', custom_data_partner_style_assignment_response)
    
                #load the json to a string
                custom_data_partner_style_assignment_response_data = json.loads(custom_data_partner_style_assignment_response)
                #print ('jsonData:', jsonData)
    
    
                #extract id element in the jsonData
                partnerStyleAssignmentContactTagId = str(custom_data_partner_style_assignment_response_data['contactTag']['id'])
                #print ('partnerStyleAssignmentContactTagId:', partnerStyleAssignmentContactTag)
                
                partnerStyleAssignmentContactTag = partnerStyleAssignmentContactTag  +","+ partnerStyleAssignmentContactTagId
                print("partnerStyleAssignmentContactTag :",partnerStyleAssignmentContactTag) 
            partnerStyleAssignmentContactTag = partnerStyleAssignmentContactTag[1:]    
            

        finalJson = json.dumps({"message":"Contact Details added successfully.","contact_id":contact_id, "fieldValues":{"genderFieldValue":genderFieldValue,"languageContactTag":languageContactTag,"styleAssignmentContactTag":styleAssignmentContactTag,"userIdFieldValue":userIdFieldValue,"compatabilityGuidesFieldValue":compatabilityGuidesFieldValue,"couplesGuidesFieldValue":couplesGuidesFieldValue,"product8DatesFieldValue":product8DatesFieldValue,"partnersFirstNameFieldValue":partnersFirstNameFieldValue,"partnersGenderFieldValue":partnersGenderFieldValue,"partnersUserIdFieldValue":partnersUserIdFieldValue,"partnerStyleAssignmentContactTag":partnerStyleAssignmentContactTag,"ageFieldValue":ageFieldValue}})
        print("finalJson :",finalJson)    
    
        return {
                    'statusCode': 200,
                    'headers':{
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                              },
                    'body': json.dumps({"message":"Contact Details added successfully.","contact_id":contact_id, "fieldValues":{"genderFieldValue":genderFieldValue,"languageContactTag":languageContactTag,"styleAssignmentContactTag":styleAssignmentContactTag,"userIdFieldValue":userIdFieldValue,"compatabilityGuidesFieldValue":compatabilityGuidesFieldValue,"couplesGuidesFieldValue":couplesGuidesFieldValue,"product8DatesFieldValue":product8DatesFieldValue,"partnersFirstNameFieldValue":partnersFirstNameFieldValue,"partnersGenderFieldValue":partnersGenderFieldValue,"partnersUserIdFieldValue":partnersUserIdFieldValue,"partnerStyleAssignmentContactTag":partnerStyleAssignmentContactTag,"ageFieldValue":ageFieldValue}})
                }
    except:
        logger.error(traceback.format_exc())
        return log_err('Data request sent in wrong format', 500)
        
if __name__== "__main__":
    handler(None,None)

