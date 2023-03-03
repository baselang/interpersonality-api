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
import datetime
from botocore.client import Config


# For getting messages according to language of the user
message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('manageemailsubscription.properties')

# secret keys for data encryption and security token
#SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

FIND_TICKET_URL = environ.get('FIND_TICKET_URL')
TICKET_FROM_EMAIL = environ.get('TICKET_FROM_EMAIL')
API_URL = environ.get('API_URL')
UPDATE_CUSTOMER_URL = environ.get('UPDATE_CUSTOMER_URL')
MESSAGE_API_URL = environ.get('MESSAGE_API_URL')
SEND_COPY_TO_CUSTOMER = environ.get('SEND_COPY_TO_CUSTOMER')
AUTHORIZATION_VALUE = environ.get('AUTHORIZATION_VALUE')
TICKET_ASSIGNEE = environ.get('TICKET_ASSIGNEE') 
UPDATE_TAGS_URL = environ.get('UPDATE_TAGS_URL')
MAILBOX_ID = environ.get('MAILBOX_ID')
SUBJECT = environ.get('SUBJECT')


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
        bodyData = json.loads(event['body'])
        name = bodyData['name']
        #body = bodyData['body']
        from_email = TICKET_FROM_EMAIL
        user_email = bodyData['email']
        subscriptionStatus = bodyData['subscriptionStatus']
        ticketId = bodyData['ticketId']
        #ticketId = "277"
        #tags = bodyData['tagType']
        #supportPriority = bodyData['supportPriority']
        send_copy_to_customer = SEND_COPY_TO_CUSTOMER
        #subject = SUBJECT
        authorization_value= AUTHORIZATION_VALUE
        #assignee = ASSIGNEE

    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
    
    print("bodyData : ",bodyData)
    # print ('tags:', tags)
    # print ('supportPriority:', supportPriority)
    print ('name:', name)
    # print ('body:', body)
    print ('from_email:', from_email)
    # print ('to_email:', to_email)
    print ('send_copy_to_customer:', send_copy_to_customer)
    # print ('subject:', subject)
    print ('authorization_value:', authorization_value)
    print ('API_URL :', API_URL)
    print ('MESSAGE_API_URL :', MESSAGE_API_URL)
    print ('ticketId :', ticketId)

    # defining a params dict for the parameters to be sent to the API rid
    HEADERS = { 'authorization':authorization_value
    } 

    print("datetime.datetime.today() :",datetime.datetime.today())
    Next29_Date = datetime.datetime.today() + datetime.timedelta(days=29)
    print (Next29_Date)
    
    # dd/mm/YY
    ExpireDate = Next29_Date.strftime("%m/%d/%Y")
    print("ExpireDate =", ExpireDate)
    
    print("TICKET_FROM_EMAIL =", TICKET_FROM_EMAIL)
    print("TICKET_ASSIGNEE =", TICKET_ASSIGNEE)
    

    if subscriptionStatus == "subscribed":
        
        if(not (ticketId and ticketId.strip())): 

            # data to be sent to api 
            ticket_data ={"name":name,
                "body":"You have subscribed to Email Upgrade and this is valid till "+ ExpireDate +". You can post your question by replying to this email.",
                "from":TICKET_FROM_EMAIL,
                "to":user_email,
                "subject":SUBJECT,
                "tags":"Email Upgrade - Subscribed",
                "send_copy_to_customer":send_copy_to_customer,
                # "starred":starred,
                "assignee":TICKET_ASSIGNEE,
                "mailbox":MAILBOX_ID
            }
            
            ticket_data1 = json.dumps(ticket_data)
            print("ticket_data1 100:",ticket_data1)
            
            try:
                # sending post request and saving response as response object 
                ticket_created_response = requests.post(url = API_URL, data = ticket_data1, headers = HEADERS) 
    
                # extracting data in json format 
                data = json.dumps(ticket_created_response.json())
                print ('data:', data)
    
                #load the json to a string
                jsonData = json.loads(data)
                print ('jsonData:', jsonData)
    
    
                #extract number element in the jsonData
                ticket_number = str(jsonData['ticket']['number'])
                print ('ticket_number:', ticket_number)
    
    
                # printing the output 
                print(ticket_created_response.status_code)  
                
                final_status = ticket_created_response.status_code
    
            except:             
                logger.info(traceback.format_exc())
                logger.info(config[message_by_language]['INTERNAL_ERROR'])
                return log_err (config[message_by_language]['INTERNAL_ERROR'], 500) 
            
        else : 
            try:
                TICKET_URL = FIND_TICKET_URL % (ticketId)   
                print("TICKET_URL :",TICKET_URL)
                
                # sending post request and saving response as response object 
                ticket_response = requests.get(url = TICKET_URL, headers = HEADERS) 
                
                # extracting data in json format 
                ticketData = json.dumps(ticket_response.json())
                print ('ticketData:', ticketData)
                
                #load the json to a string
                jsonTicketData = json.loads(ticketData)
                print ('jsonTicketData:', jsonTicketData)
                
                
                #extract number element in the jsonData
                ticketTag = str(jsonTicketData['ticket']['tags'][0])
                print ('ticketTag:', ticketTag)
                
            except:             
                logger.info(traceback.format_exc())
                logger.info(config[message_by_language]['INTERNAL_ERROR'])
                return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
        
            if (ticketTag == "Email Upgrade - Expired"):
                
                # data to be sent to api 
                tag_data ={"tags":["Email Upgrade - Subscribed"]
                }
                        
                tag_data = json.dumps(tag_data)
                print("tag_data 100:",tag_data)
                
                TAG_URL = UPDATE_TAGS_URL % (ticketId)    
                
                try:
                    # sending post request and saving response as response object 
                    response = requests.put(url = TAG_URL, data = tag_data, headers = HEADERS) 
        
                    # extracting data in json format 
                    print ('response:', response)
        
                except:             
                    logger.info(traceback.format_exc())
                    logger.info(config[message_by_language]['INTERNAL_ERROR'])
                    return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
                
                # message data to be sent to api 
                msg_data ={
                    "body":"You have subscribed to Email Upgrade and this is valid till "+ ExpireDate +". You can post your question by replying to this email.",
                "send_copy_to_customer":send_copy_to_customer
                }
               
                #Send Message Body.       
                try:
                    # sending post request and saving response as response object
                    ticket_number = ticketId 
                    URL = MESSAGE_API_URL % (ticket_number)    
                    msg_response = requests.post(url = URL, data = msg_data, headers = HEADERS) 
                    # extracting data in json format 
                    msg_data = json.dumps(msg_response.json())
                    print ('msg_data:', msg_data)
        
                    #load the json to a string
                    msg_jsonData = json.loads(msg_data)
                    print ('msg_jsonData:', msg_jsonData)
        
                    # printing the output 
                    print(msg_response.status_code)  
                    
                    final_status = msg_response.status_code
        
                except:             
                    logger.info(traceback.format_exc())
                    logger.info(config[message_by_language]['INTERNAL_ERROR'])
                    return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
                    
            else:
                return {
                'statusCode': 200,
                'headers':
                    {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Credentials': 'true'
                    },
                'body': json.dumps({"message":"","ticketId":ticketId})
                }   

    else:
        
        ticketId = bodyData['ticketId']
        print(" ticketId :",ticketId)
        
        # data to be sent to api 
        tag_data ={"tags":["Email Upgrade - Expired"]
        }
                
        tag_data = json.dumps(tag_data)
        print("tag_data 100:",tag_data)
        
        ticket_number = ticketId 
        TAG_URL = UPDATE_TAGS_URL % (ticket_number)    
        
        try:
            # sending post request and saving response as response object 
            response = requests.put(url = TAG_URL, data = tag_data, headers = HEADERS) 

            # extracting data in json format 
            print ('response:', response)

        except:             
            logger.info(traceback.format_exc())
            logger.info(config[message_by_language]['INTERNAL_ERROR'])
            return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)    


        # message data to be sent to api 
        msg_data ={
            "body":"Your Email Upgrade now has been expired. To renew this, please login to your account.",
        "send_copy_to_customer":send_copy_to_customer
        }
           
        #Send Message Body.       
        try:
            # sending post request and saving response as response object
            ticket_number = ticketId 
            URL = MESSAGE_API_URL % (ticket_number)    
            msg_response = requests.post(url = URL, data = msg_data, headers = HEADERS) 
            # extracting data in json format 
            msg_data = json.dumps(msg_response.json())
            print ('msg_data:', msg_data)

            #load the json to a string
            msg_jsonData = json.loads(msg_data)
            print ('msg_jsonData:', msg_jsonData)

            # printing the output 
            print(msg_response.status_code)  
            
            final_status = msg_response.status_code

        except:             
            logger.info(traceback.format_exc())
            logger.info(config[message_by_language]['INTERNAL_ERROR'])
            return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
  
   
    if (final_status == 201):
        # print("msg_response.status_code :",msg_response.status_code)
        # print("ticket_created_response.status_code :",ticket_created_response.status_code)
        ticket_link_url = "https://certainty-infotech.groovehq.com/tickets/" + ticket_number
        print("ticket_link_url :",ticket_link_url)
        ticket_message = "<a href=\""+ ticket_link_url +"\">Click here to see ticket.</a>"
        #message = "Subscription "+ subscriptionStatus +" successfully."
        print("ticket_message :",ticket_message)
        return {
            'statusCode': 200,
            'headers':
                {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
            'body': json.dumps({"message":ticket_message,"ticketId":ticket_number})
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