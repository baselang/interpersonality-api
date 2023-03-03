#!/usr/bin/env python3

"""API Module to Create Git Hub Issue Functionalities.

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
import boto3
from botocore.client import Config
import base64

# For getting messages according to language of the user
message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('creategithubissue.properties')

# secret keys for data encryption and security token
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

# secret keys for image upload for creating boto3 client
ACCESS_KEY_ID = environ.get('ACCESS_KEY_ID')
SECRET_ACCESS_KEY = environ.get('SECRET_ACCESS_KEY')
BUCKET_NAME = environ.get('BUCKET_NAME')
S3_BUCKET_URL = environ.get('S3_BUCKET_URL')

USERNAME = environ.get('USERNAME')
PASSWORD = environ.get('PASSWORD')
REPO_OWNER = environ.get('REPO_OWNER')
REPO_NAME = environ.get('REPO_NAME')
ASSIGNEES = environ.get('ASSIGNEES')
GIT_HUB_CREATE_ISSUE_URL = environ.get('GIT_HUB_CREATE_ISSUE_URL')
GIT_HUB_COMMENT_URL = environ.get('GIT_HUB_COMMENT_URL')
GIT_HUB_UPDATE_ISSUE_URL = environ.get('GIT_HUB_UPDATE_ISSUE_URL')


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
            
def upload_image(git_hub_issue_number, image_data):
    """Function to upload image to S3 and generate url"""
    # creating boto3 client 
    S3 = boto3.resource(
        's3',
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4')
        )
    
    # print ('ACCESS_KEY_ID :', ACCESS_KEY_ID)
    # print ('SECRET_ACCESS_KEY :', SECRET_ACCESS_KEY)
    # print ('BUCKET_NAME :', BUCKET_NAME)
    # print ('S3_BUCKET_URL :', S3_BUCKET_URL)
    # print ('S3:', S3)
    # print ('git_hub_issue_number:', git_hub_issue_number)

    # splitting user provided image data to convert into image format
    ans = image_data.split("base64,", 1)
    image_data = ans[1]
    
    # converting string to byte format
    image_data = image_data.encode("utf-8")
    
    # converting byte format to base64 format
    image_data = base64.decodebytes(image_data)
    
    # uploading image to S3 bucket
    response = S3.Object(BUCKET_NAME,git_hub_issue_number + ".png").put(Body=image_data, ACL='public-read-write',  ContentType='image/png')
    logger.info(response)
    
    # returning S3 url of the image
    return S3_BUCKET_URL + git_hub_issue_number + ".png"


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
        print ('data:', data)
        #title = data["title"]
        body = data['body']
        labels = data['labels']
        assignees = ASSIGNEES
        picture_data = data['picture_data']
        username = data['username']
        email = data['email']
        issueType = data['issueType']
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
    
    # print ('title:', title)
    # print ('body:', body)
    # print ('labels:', labels)
    # #print ('assignees:', assignees)
    #print ('picture_data:', picture_data)
    # print ('ASSIGNEES:', ASSIGNEES)
    

    try:
        url = GIT_HUB_CREATE_ISSUE_URL % (REPO_OWNER, REPO_NAME)
        print ('url:', url)

        # Create an authenticated session to create the issue
        session = requests.Session()
        session.auth = (USERNAME, PASSWORD)

        # Issue Json
        issueData ={"title": "Bug",
            "body": body,
            "labels": labels,
            "assignee": assignees
        }

        # Add the issue to our repository
        issue_response = session.post(url, json.dumps(issueData))
        print ('issue_response.status_code:', issue_response.status_code)
    
        data = json.dumps(issue_response.json())
        print ('data:', data)

        #load the json to a string
        jsonData = json.loads(data)
        print ('jsonData:', jsonData)

        #extract an element in the response
        git_hub_issue_number = str(jsonData['number'])
        print ('git_hub_issue_number:', git_hub_issue_number)
    except:
        logger.info(traceback.format_exc())
        return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
        
    try:
        url = GIT_HUB_UPDATE_ISSUE_URL % (REPO_OWNER, REPO_NAME,git_hub_issue_number)    
        print ('url:', url)
        
        # Update Issue Json
        updateIssueData ={"title": "Bug "+git_hub_issue_number,
            "body": body,
            "labels": labels,
            "assignee": assignees
        }
        
        # Add the update issue to our repository
        update_issue_response = session.post(url, json.dumps(updateIssueData))
        print ('update_issue_response.status_code:', update_issue_response.status_code)
    except:
        logger.info(traceback.format_exc())
        return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
    
    url = GIT_HUB_COMMENT_URL % (REPO_OWNER, REPO_NAME,git_hub_issue_number)
    print ('url:', url)
    

    # comment json
    commentData ={
        "body": "Username : " + username + ", Email : " + email +" , Issue Type : " + issueType 
    }

    # Add the comment to issue
    comment_response = session.post(url, json.dumps(commentData))

    try:
        # uploading picture to s3 if it is not the same as old and generating url of it
        picture_url = upload_image(git_hub_issue_number, picture_data)
        issue_url = "Screenshot : \n ![Issue]("+picture_url+")"
        #picture_url_prefix = "image-url : "
        # comment json
        commentData1 ={
            "body": issue_url
        }

        # Add the comment to issue
        comment1_response = session.post(url, json.dumps(commentData1))
        print ('comment_response.status_code:', comment_response.status_code)
    
        commentData1 = json.dumps(comment_response.json())
        print ('commentData:', commentData1)

        #load the json to a string
        commentJsonData1 = json.loads(commentData1)
        print ('commentJsonData1:', commentJsonData1)
    except:             
        logger.info(traceback.format_exc())
        logger.info(config[message_by_language]['IMAGE_STATUS'])
        #return log_err (config[message_by_language]['IMAGE_STATUS'], 500)
    
    if issue_response.status_code == 201:
            issue_link_url = "https://github.com/fourman-personality/profiles-api/issues/" + git_hub_issue_number
            print("issue_link_url :",issue_link_url)
            issue_message = "<a href=\""+ issue_link_url +"\">Click here to see issue.</a>"
            #issue_message = "Issue created successfully. Please find issue URL ->  "+ issue_link_url +""
            print("issue_message :",issue_message)
            return {
                    'statusCode': 200,
                    'headers':
                        {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                        },
                    #'body': json.dumps({"message":"Issue Created successfully. " <a href="https://github.com/fourman-personality/profiles-api/issues/" + git_hub_issue_number ">Click here for issue detail</a>"})
                    #'body': json.dumps({"message":"Issue created successfully. Click <a href=\"https://github.com/hareesh2024/Demo-Repo/issues/66\">here to view issue detail.</a>"})
                    'body': json.dumps({"message":issue_message})
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
           #return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)

if __name__== "__main__":
    handler(None,None)