"""
API Module to delete contact in active campaign

It provides the following functionalities:
1. log_err(): Logging error and returning the JSON response with error message & status code
2. handler(): Handling the incoming request with following steps:
- Fetching data required for api
- returning the success json with contact id

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
# config.read('deletecontactinactivecampaign.properties')

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

    data = json.loads(event['body'])
    contact_id = data["contact_id"]
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

    
    fieldValues = data['fieldValues']
    
    print("fieldValues :",fieldValues)
    
    print(fieldValues['genderFieldValue'])


    languageContactTag = fieldValues["languageContactTag"]
    styleAssignmentContactTag = fieldValues["styleAssignmentContactTag"]
    partnerStyleAssignmentContactTag = fieldValues["partnerStyleAssignmentContactTag"]
    print("partnerStyleAssignmentContactTag",partnerStyleAssignmentContactTag)

    if(is_8_dates == "Yes"):
        partners_first_name = data["partners_first_name"]
        partners_gender = data["partners_gender"]
        partners_user_id = data["partners_user_id"]
        partners_style_assignment = data["partner_style_assignment"]
        partnerStyleAssignmentContactTag = fieldValues["partnerStyleAssignmentContactTag"]
  
    language_dict = {"English": 6, "Spanish": 7} 
    #style_assignment_dict = {"0101": 8, "0102": 9,"0103": 10,"0104": 11,"0105": 12,"0106": 13,"0201": 14, "0202": 15,"0203": 16,"0301": 17, "0302": 18,"0401": 19, "0402": 20,"0403": 21,"0501": 22, "0502": 23,"0503": 24,"0601": 25}
    style_assignment_dict = {"0101": 8, "0102": 9,"0103": 10,"0104": 11,"0105": 12,"0106": 13,"0201": 14, "0202": 15,"0203": 16,"0301": 17, "0302": 18,"0401": 19, "0402": 20,"0403": 21,"0501": 22, "0502": 23,"0503": 24,"0601": 25,"0602": 26,"0701": 27, "0702": 28,"0801": 29, "0802": 30,"0803": 31,"0804": 32,"0901": 33, "0902": 34,"1001": 35, "1002": 36,"1003": 37,"1004": 38,"1005": 39,"1101": 40, "1102": 41,"1103": 42,"1201": 43, "1202": 44,"1203": 45,"1301": 46, "1302": 47,"1303": 48,"1304": 49, "1305": 50,"1306": 51,"1307": 52, "1308": 53,"1401": 54, "1402": 55,"1403": 56,"1404": 57, "1405": 58,"1406": 59,"1407": 60, "1408": 61}


    language_id = language_dict[language]

    # if(is_8_dates == "Yes"):
    #     print ('partners_first_name:', partners_first_name)
    #     print ('partners_gender:', partners_gender)
    #     print ('partners_user_id:', partners_user_id)
    #     print ('partners_style_assignment:', partners_style_assignment)
    #     print ('partnerStyleAssignmentContactTag:', partnerStyleAssignmentContactTag)

    # print ('language_id:', language_id)
    # print ('age:', age)
    # print ('contact_id:', contact_id)
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
    # print ('languageContactTag:', languageContactTag)
    # print ('styleAssignmentContactTag:', styleAssignmentContactTag)

    # defining a params dict for the parameters to be sent to the API rid
    HEADERS = { 'Api-Token':'213988f23b751601cb3161d22386c73ce2a45c0437d5802ca0fcd23cf0ba86f49a636ac8'
    } 

    ADD_CONTACT_URL = 'https://certaintyinfotech.api-us1.com/api/3/contact/sync'
    UPDATE_CONTACT_URL = 'https://certaintyinfotech.api-us1.com/api/3/contacts/'
    ADD_CUSTOM_FIELD_VALUE_URL = 'https://certaintyinfotech.api-us1.com/api/3/fieldValues'
    ADD_TAG_URL = 'https://certaintyinfotech.api-us1.com/api/3/contactTags'
    REMOVE_TAG_URL = 'https://certaintyinfotech.api-us1.com/api/3/contactTags/'
    UPDATE_CUSTOM_FIELD_VALUE_URL = 'https://certaintyinfotech.api-us1.com/api/3/fieldValues/'
    DELETE_CUSTOM_FIELD_VALUE_URL = 'https://certaintyinfotech.api-us1.com/api/3/fieldValues/'
    GET_CONTACT_TAG_URL = 'https://certaintyinfotech.api-us1.com/api/3/contacts/'
    DELETE_CONTACT_URL = 'https://certaintyinfotech.api-us1.com/api/3/contacts/'
    
    response = requests.get(url = GET_CONTACT_TAG_URL+'%s' % (contact_id), headers = HEADERS)
    
    print("response :",response)
    #convert to string.
    contactData = json.dumps(response.json())
    print ('contactData:', contactData)

    #convert to json.
    contactJsonData = json.loads(contactData)

    print("contactJsonData :",contactJsonData)

    # #extract fieldValues element in the contactJsonData
    fieldValues = contactJsonData['fieldValues'][0]   
    print ('fieldValues:-', fieldValues)

    lengthFieldValues = len(contactJsonData['fieldValues'])

    for i in range(lengthFieldValues):
        print(i)
        print("Contact Field Data :",contactJsonData['fieldValues'][i])
        print("Contact field Id:",contactJsonData['fieldValues'][i]['field'])
        print("Contact fieldValues :",contactJsonData['fieldValues'][i]['id'])

        if(int(contactJsonData['fieldValues'][i]['field']) == 20):            
            genderFieldValue = contactJsonData['fieldValues'][i]['id']
        
        elif(int(contactJsonData['fieldValues'][i]['field']) == 4):    
            userIdFieldValue = contactJsonData['fieldValues'][i]['id']
        
        elif(int(contactJsonData['fieldValues'][i]['field']) == 5):
            compatabilityGuidesFieldValue = contactJsonData['fieldValues'][i]['id']
        
        elif(int(contactJsonData['fieldValues'][i]['field']) == 6):
            couplesGuidesFieldValue = contactJsonData['fieldValues'][i]['id']
        
        elif(int(contactJsonData['fieldValues'][i]['field']) == 7):
            product8DatesFieldValue = contactJsonData['fieldValues'][i]['id']
        
        elif(int(contactJsonData['fieldValues'][i]['field']) == 16):
            partnersFirstNameFieldValue = contactJsonData['fieldValues'][i]['id']
        
        elif(int(contactJsonData['fieldValues'][i]['field']) == 17):
            partnersGenderFieldValue = contactJsonData['fieldValues'][i]['id']

        elif(int(contactJsonData['fieldValues'][i]['field']) == 18):
            partnersUserIdFieldValue = contactJsonData['fieldValues'][i]['id']

        elif(int(contactJsonData['fieldValues'][i]['field']) == 21):
            ageFieldValue = contactJsonData['fieldValues'][i]['id']    

    print ('genderFieldValue:', genderFieldValue)
    print ('userIdFieldValue:', userIdFieldValue)
    print ('ageFieldValue:', ageFieldValue)
    print ('compatabilityGuidesFieldValue:', compatabilityGuidesFieldValue)
    print ('couplesGuidesFieldValue:', couplesGuidesFieldValue)
    print ('product8DatesFieldValue:', product8DatesFieldValue)    

    print ('partnersFirstNameFieldValue:', partnersFirstNameFieldValue)
    print ('partnersGenderFieldValue:', partnersGenderFieldValue)
    print ('partnersUserIdFieldValue:', partnersUserIdFieldValue)
    try:
        
        # sending post request and saving response as response object 
        response = requests.delete(url = DELETE_CONTACT_URL+'%s' % (contact_id), headers = HEADERS)
        # preparing success json  with result_list
        print("Delete contact Details response :",response)        
        print ('contact_id:', contact_id)
        
        response = requests.delete(url = DELETE_CUSTOM_FIELD_VALUE_URL+'%s' % (genderFieldValue), headers = HEADERS)
        # preparing success json  with result_list
        print("delete genderFieldValue response :",response)
        
        response = requests.delete(url = DELETE_CUSTOM_FIELD_VALUE_URL+'%s' % (ageFieldValue), headers = HEADERS)
        # preparing success json  with result_list
        print("delete ageFieldValue response :",response)

        try:
            response = requests.delete(url = REMOVE_TAG_URL+'%s' % (languageContactTag), headers = HEADERS)
            #response = requests.request("DELETE", DELETE_TAG_URL)
            print("remove languageContactTag response :",response)
            print("remove languageContactTag  :",languageContactTag)
        except:
            print("Problem for deleting tag.")

        try:
            styleAssignmentTagIdForDelete = styleAssignmentContactTag.split(",")
            print("styleAssignmentTagIdForDelete :",styleAssignmentTagIdForDelete) 
            for tagId in styleAssignmentTagIdForDelete:
                response = requests.delete(url = REMOVE_TAG_URL+'%s' % (tagId), headers = HEADERS)
                print("response :",response)
                print("remove tagId :",response)
                print("tagId :",tagId)
                styleAssignmentContactTag = ''
        except:
            print("Problem for deleting tag.")

        response = requests.delete(url = DELETE_CUSTOM_FIELD_VALUE_URL+'%s' % (userIdFieldValue), headers = HEADERS)
        # preparing success json  with result_list
        print("delete userIdFieldValue  response:",response)

        response = requests.delete(url = DELETE_CUSTOM_FIELD_VALUE_URL+'%s' % (compatabilityGuidesFieldValue), headers = HEADERS)
        # preparing success json  with result_list
        print("delete compatabilityGuidesFieldValue  response:",response)

        response = requests.delete(url = DELETE_CUSTOM_FIELD_VALUE_URL+'%s' % (couplesGuidesFieldValue), headers = HEADERS)
        # preparing success json  with result_list
        print("delete couplesGuidesFieldValue  response:",response)

        response = requests.delete(url = DELETE_CUSTOM_FIELD_VALUE_URL+'%s' % (product8DatesFieldValue), headers = HEADERS)
        # preparing success json  with result_list
        print("delete product8DatesFieldValue  response:",response)

        if(is_8_dates == "Yes"):
            response = requests.delete(url = DELETE_CUSTOM_FIELD_VALUE_URL+'%s' % (partnersFirstNameFieldValue), headers = HEADERS)
            # preparing success json  with result_list
            print("delete partnersFirstNameFieldValue  response:",response)


            response = requests.delete(url = DELETE_CUSTOM_FIELD_VALUE_URL+'%s' % (partnersGenderFieldValue), headers = HEADERS)
            # preparing success json  with result_list
            print("delete partnersGenderFieldValue  response:",response)


            response = requests.delete(url = DELETE_CUSTOM_FIELD_VALUE_URL+'%s' % (partnersUserIdFieldValue), headers = HEADERS)
            # preparing success json  with result_list
            print("delete partnersUserIdFieldValue  response:",response)

            try:
                partnerStyleAssignmentTagIdForDelete = partnerStyleAssignmentContactTag.split(",")
                print("partnerStyleAssignmentTagIdForDelete :",partnerStyleAssignmentTagIdForDelete) 
                for tagId in partnerStyleAssignmentTagIdForDelete:
                    response = requests.delete(url = REMOVE_TAG_URL+'%s' % (tagId), headers = HEADERS)
                    #response = requests.request("DELETE", DELETE_TAG_URL)
                    print("response :",response)
                    print("remove tagId :",response)
                    print("tagId :",tagId)
                    partnerStyleAssignmentContactTag = ''
            except:
                print("Problem for deleting tag.")

        elif(is_8_dates == "No"):
            
            if( partnersFirstNameFieldValue != ''):
                
                response = requests.delete(url = DELETE_CUSTOM_FIELD_VALUE_URL+'%s' % (partnersFirstNameFieldValue), headers = HEADERS)
                # preparing success json  with result_list
                print("partnersFirstNameFieldValue delete :",response)
                partnersFirstNameFieldValue = ''
                
            if(partnersGenderFieldValue != ''):                
                
                response = requests.delete(url = DELETE_CUSTOM_FIELD_VALUE_URL+'%s' % (partnersGenderFieldValue), headers = HEADERS)
                # preparing success json  with result_list
                print("partnersGenderFieldValue delete :",response)
                partnersGenderFieldValue = ''
                
            if(partnersUserIdFieldValue != ''):
               
                response = requests.delete(url = DELETE_CUSTOM_FIELD_VALUE_URL+'%s' % (partnersUserIdFieldValue), headers = HEADERS)
                # preparing success json  with result_list
                print("partnersUserIdFieldValue delete :",response)
                partnersUserIdFieldValue = ''
               
            if(partnerStyleAssignmentContactTag != ''):
                try:
                    partnerStyleAssignmentTagIdForDelete = partnerStyleAssignmentContactTag.split(",")
                    print("partnerStyleAssignmentTagIdForDelete :",partnerStyleAssignmentTagIdForDelete) 
                    for tagId in partnerStyleAssignmentTagIdForDelete:
                        response = requests.delete(url = REMOVE_TAG_URL+'%s' % (tagId), headers = HEADERS)
                        #response = requests.request("DELETE", DELETE_TAG_URL)
                        print("response :",response)
                        print("remove tagId :",response)
                        print("tagId :",tagId)
                        partnerStyleAssignmentContactTag = ''
                except:
                    print("Problem for deleting tag.")      

        finalJson = json.dumps({"message":"Contact Details Deleted successfully.","contact_id":contact_id, "fieldValues":{"genderFieldValue":genderFieldValue,"languageContactTag":languageContactTag,"styleAssignmentContactTag":styleAssignmentContactTag,"userIdFieldValue":userIdFieldValue,"compatabilityGuidesFieldValue":compatabilityGuidesFieldValue,"couplesGuidesFieldValue":couplesGuidesFieldValue,"product8DatesFieldValue":product8DatesFieldValue,"partnersFirstNameFieldValue":partnersFirstNameFieldValue,"partnersGenderFieldValue":partnersGenderFieldValue,"partnersUserIdFieldValue":partnersUserIdFieldValue,"partnerStyleAssignmentContactTag":partnerStyleAssignmentContactTag,"ageFieldValue":ageFieldValue}})
        print("finalJson :",finalJson)

        return {
                    'statusCode': 200,
                    'headers':{
                                'Access-Control-Allow-Origin': '*',
                                'Access-Control-Allow-Credentials': 'true'
                              },
                    'body': json.dumps({"message":"Contact Details Deleted successfully.","contact_id":contact_id, "fieldValues":{"genderFieldValue":genderFieldValue,"languageContactTag":languageContactTag,"styleAssignmentContactTag":styleAssignmentContactTag,"userIdFieldValue":userIdFieldValue,"compatabilityGuidesFieldValue":compatabilityGuidesFieldValue,"couplesGuidesFieldValue":couplesGuidesFieldValue,"product8DatesFieldValue":product8DatesFieldValue,"partnersFirstNameFieldValue":partnersFirstNameFieldValue,"partnersGenderFieldValue":partnersGenderFieldValue,"partnersUserIdFieldValue":partnersUserIdFieldValue,"partnerStyleAssignmentContactTag":partnerStyleAssignmentContactTag,"ageFieldValue":ageFieldValue}})
                }
    except:
        logger.error(traceback.format_exc())
        return log_err('Data request sent in wrong format', 500)
        
if __name__== "__main__":
    handler(None,None)

