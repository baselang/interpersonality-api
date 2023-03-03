#!/usr/bin/env python3

"""API Module to Create Groove Ticket Functionalities.

It provides the following functionalities:
1. log_err(): Logging error and returning the JSON response with error message & status code
2. jwt_verify(): verifying token and fetching data from the jwt token sent by user
3. handler(): Handling the incoming request with following steps:
- Update Privacy Settings 
- Returning the JSON response with success status code

"""
import os
import json
import requests
import logging
import traceback
from os import environ
import configparser
from botocore.client import Config


# For getting messages according to language of the user
message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('creategrooveticket.properties')

# secret keys for data encryption and security token
#SECRET_KEY = environ.get('TOKEN_SECRET_KEY')



FROM_EMAIL = environ.get('FROM_EMAIL')
MAILBOX_ID = environ.get('MAILBOX_ID')
API_URL = environ.get('API_URL')
MESSAGE_API_URL = environ.get('MESSAGE_API_URL')
SEND_COPY_TO_CUSTOMER = environ.get('SEND_COPY_TO_CUSTOMER')
SUBJECT = environ.get('SUBJECT')
AUTHORIZATION_VALUE = environ.get('AUTHORIZATION_VALUE')
ASSIGNEE = environ.get('ASSIGNEE') 


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
    """Function to handle the request for Update Privacy Settings API."""
    
    global message_by_language
    logger.info(event)
    # try:
    #     # fetching language_id from the event data
    #     auth_token = event['headers']['Authorization']
    # except:
    #     # If there is any error in above operations, logging the error
    #     return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
        
    # try:
    #     # verifying that the user is authorized or not to see this api's data
    #     rid, user_id, language_id = jwt_verify(auth_token)
    # except:
    #     # if user does not have valid authorization
    #     logger.error(traceback.format_exc())
    #     return log_err(config[message_by_language]['UNAUTHORIZED'], 403)

    #Fetching data from event body
    try:
    # Fetching data from event and rendering it
        data = json.loads(event['body'])
        name = data['name']
        body = data['body']
        from_email = FROM_EMAIL
        to_email = data['email']
        tags = data['tagType']
        supportPriority = data['supportPriority']
        send_copy_to_customer = SEND_COPY_TO_CUSTOMER
        subject = SUBJECT
        authorization_value= AUTHORIZATION_VALUE
        assignee = ASSIGNEE

    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
    
    print("data : ",data)
    print ('tags:', tags)
    print ('supportPriority:', supportPriority)
    print ('name:', name)
    print ('body:', body)
    print ('from_email:', from_email)
    print ('to_email:', to_email)
    print ('send_copy_to_customer:', send_copy_to_customer)
    print ('subject:', subject)
    print ('authorization_value:', authorization_value)
    print ('API_URL :', API_URL)
    print ('MESSAGE_API_URL :', MESSAGE_API_URL)
    print ('ASSIGNEE :', assignee)
    print ('MAILBOX_ID :', MAILBOX_ID)

    # defining a params dict for the parameters to be sent to the API rid
    HEADERS = { 'authorization':authorization_value
    } 

    if (supportPriority == "high" and  tags == "Support"):
        tags = "Support"
        print("tags value 101 :",tags)
        
    if (supportPriority == "low" and  tags == "Support"):
        tags = "Support - Free User"
        print("tags value 102 :",tags)
        
    if (supportPriority == "low" and  tags == "Billing Issue"):
        return {
            'statusCode': 200,
            'headers':
                {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
            'body': json.dumps({"message":""})
        }    
        
    if ( tags == "Feedback & Suggestions" or tags == "General Contact"):
        #starred = True
        #print("Condition value :",starred) 
        
        # data to be sent to api 
        data ={"name":name,
            "body":body,
            "from":from_email,
            "to":to_email,
            "subject":subject,
            "tags":tags,
            # "starred":starred,
            "mailbox":MAILBOX_ID,
            "assignee":assignee
        }
    
    
        data1 = json.dumps(data)

        print("data1 100:",data1)
    
        try:
            # sending post request and saving response as response object 
            response = requests.post(url = API_URL, data = data1, headers = HEADERS) 

            # extracting data in json format 
            data = json.dumps(response.json())
            print ('data:', data)

            #load the json to a string
            jsonData = json.loads(data)
            print ('jsonData:', jsonData)


            #extract number element in the jsonData
            ticket_number = str(jsonData['ticket']['number'])
            print ('ticket_number:', ticket_number)


            # printing the output 
            print(response.status_code)  

        except:             
            logger.info(traceback.format_exc())
            logger.info(config[message_by_language]['INTERNAL_ERROR'])
            return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)      
    else: 
        if supportPriority == "low":
    	    #starred = False
            data = {"name":name,
                "body":body,
                "from":from_email,
                "to":to_email,
                "subject":subject,
                "tags":tags,
                # "starred":starred,
                "mailbox":MAILBOX_ID,
                "assignee":assignee
            }	    
            data1 = json.dumps(data)
            print("data1 100:",data1)
            try:
                # sending post request and saving response as response object 
                response = requests.post(url = API_URL, data = data1, headers = HEADERS)
                # extracting data in json format 
                data = json.dumps(response.json())
                print ('data:', data)
                #load the json to a string
                jsonData = json.loads(data)
                print ('jsonData:', jsonData)
                #extract number element in the jsonData
                ticket_number = str(jsonData['ticket']['number'])
                print ('ticket_number:', ticket_number)
                # printing the output 
                print(response.status_code)  
            except:             
                logger.info(traceback.format_exc())
                logger.info(config[message_by_language]['INTERNAL_ERROR'])
                return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
            # # message data to be sent to api 
            # msg_data ={
            #     "body":"We will try to get to your issue, but as we have lots of users, you may not get a reply.",
            # "send_copy_to_customer":send_copy_to_customer
            # }
               
            # #Send Message Body.       
            # try:
            #     # sending post request and saving response as response object 
            #     URL = MESSAGE_API_URL % (ticket_number)    
            #     msg_response = requests.post(url = URL, data = msg_data, headers = HEADERS) 
            #     # extracting data in json format 
            #     msg_data = json.dumps(msg_response.json())
            #     print ('msg_data:', msg_data)
            
            #     #load the json to a string
            #     msg_jsonData = json.loads(msg_data)
            #     print ('msg_jsonData:', msg_jsonData)

            #     # printing the output 
            #     print(msg_response.status_code)  

            # except:             
            #     logger.info(traceback.format_exc())
            #     logger.info(config[message_by_language]['INTERNAL_ERROR'])
            #     return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)     
    	    
        else: 
    	    #starred = True
    	    #print("Condition value :",starred) 
    	    
            # data to be sent to api 
            data ={"name":name,
                "body":body,
                "from":from_email,
                "to":to_email,
                "send_copy_to_customer":send_copy_to_customer,
                "subject":subject,
                "tags":tags,
                # "starred":starred,
                "mailbox":MAILBOX_ID,
                "assignee":assignee
            }
        
        
            data1 = json.dumps(data)

            print("data1 100:",data1)
        
            try:
                # sending post request and saving response as response object 
                response = requests.post(url = API_URL, data = data1, headers = HEADERS) 

                # extracting data in json format 
                data = json.dumps(response.json())
                print ('data:', data)

                #load the json to a string
                jsonData = json.loads(data)
                print ('jsonData:', jsonData)


                #extract number element in the jsonData
                ticket_number = str(jsonData['ticket']['number'])
                print ('ticket_number:', ticket_number)


                # printing the output 
                print(response.status_code)  

            except:             
                logger.info(traceback.format_exc())
                logger.info(config[message_by_language]['INTERNAL_ERROR'])
                return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
   
    if response.status_code == 201:
        ticket_link_url = "https://certainty-infotech.groovehq.com/tickets/" + ticket_number
        print("ticket_link_url :",ticket_link_url)
        ticket_message = "<a href=\""+ ticket_link_url +"\">Click here to see ticket.</a>"
        print("ticket_message :",ticket_message)
        return {
            'statusCode': 200,
            'headers':
                {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
            'body': json.dumps({"message":ticket_message})
        }    
    else:
        return {
            'statusCode': 500,
            'headers':
                {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
            'body': json.dumps({"message":"Request Failed"})
        }

if __name__== "__main__":
    handler(None,None)