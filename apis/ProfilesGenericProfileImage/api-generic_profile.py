"""API Module to be called asynchronously from Profiles page to generate an image.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. upload_image() : Function to upload image to s3 bucket
3. generate_image() : Function to generate the generic image for sharing through facebook on similarity page on friends profile
4. log_err(): Logging error and returning the JSON response with error message & status code
5. round_corner_jpg(): Function to generate round corner for image
6. crop_image(): Function to crop and resize the image
7. handler(): Handling the incoming request with following steps:
- Fetching user_profile details from event
- getting the user additional details to generate image and than calling the generate image
- Returning the JSON response with success status code

"""

import json
import pymysql
import logging
import traceback
from os import environ
import configparser
from datetime import datetime
from PIL import Image, ImageDraw, ImageFilter, ImageFont
import aggdraw
import io
import boto3
from botocore.client import Config
from urllib.request import urlopen

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('generic_profile.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# images that needs to be imported from s3
background_img_url = environ.get('BACKGROUND_IMG_URL')
background_img_url_long = environ.get('BACKGROUND_IMG_URL_LONG')
background_img_url_es = environ.get('BACKGROUND_IMG_URL_ES')
background_img_url_no_pic = environ.get('BACKGROUND_IMG_URL_NO_PIC')
background_img_url_no_pic_long =environ.get('BACKGROUND_IMG_URL_NO_PIC_LONG')
background_img_url_no_pic_es = environ.get('BACKGROUND_IMG_URL_NO_PIC_ES')
DEFAULT_IMAGE = environ.get('DEFAULT_IMAGE')

# variables that can be used for text and image formatting
im_size = int(environ.get('IMG_SIZE'))
upper_text = environ.get('UPPER_TEXT')
upper_text_no_s = environ.get('UPPER_TEXT_NO_S')
upper_text_long = environ.get('UPPER_TEXT_LONG')
upper_text_long_no_s = environ.get('UPPER_TEXT_LONG_NO_S')
upper_text_es = environ.get('UPPER_TEXT_ES')
text_1_font = environ.get('TEXT_1_FONT')
text_1_size = int(environ.get('TEXT_1_SIZE'))
image_template = environ.get('IMAGE_TEMPLATE')

# getting width and height of the background_img
w = int(environ.get('IMG_WIDTH'))
h = int(environ.get('IMG_HEIGHT'))

# Variables related to s3 bucket
BUCKET_URL = environ.get('BUCKET_URL')
BUCKET_NAME = environ.get('BUCKET_NAME')
ACCESS_KEY_ID = environ.get('ACCESS_KEY_ID')
SECRET_ACCESS_KEY = environ.get('SECRET_ACCESS_KEY')
SUB_FOLDER = environ.get('SUB_FOLDER')


# Getting key for getting token
key = environ.get('DB_ENCRYPTION_KEY')

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


def log_err(errmsg):
    """Function to log the error messages."""
    return {
        "statusCode": 400,
        "body": json.dumps({"message": errmsg}),
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': 'true'},
        "isBase64Encoded": "false"
    }


def upload_image(data, user_id, language_id):
    """Function for uploading image to S3 Bucket"""
    try:
        # creating binary input output stream
        image_bytes = io.BytesIO()

        # converting image into binary data and save into io stream
        data.save(image_bytes, format="PNG")

        # getting stored binary value for image from io stream
        image_bytes = image_bytes.getvalue()

        # creating a boto3 resource
        S3 = boto3.resource(
            's3',
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=SECRET_ACCESS_KEY,
            config=Config(signature_version='s3v4')
        )

        # creating name of the image
        image_name = image_template.format(language_id, str(user_id))
        # uploading object to s3 bucket
        response = S3.Object(BUCKET_NAME, SUB_FOLDER + image_name).put(Body=image_bytes, ACL='public-read-write')

        # returning the url of the image
        return BUCKET_URL + image_name
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['UPLOAD_STATUS'])


def round_corner_jpg(image, radius):
    """function to generate round corner for image"""
    mask = Image.new('L', image.size)  # filled with black by default
    draw = aggdraw.Draw(mask)
    brush = aggdraw.Brush('white')
    width, height = mask.size

    # upper-left corne
    draw.pieslice((0, 0, radius * 2, radius * 2), 90, 180, None, brush)
 
    # upper-right corner
    draw.pieslice((width - radius * 2, 0, width, radius * 2), 0, 90, None, brush)
 
    # bottom-left corner
    draw.pieslice((0, height - radius * 2, radius * 2, height), 180, 270, None, brush)
 
    # bottom-right corner
    draw.pieslice((width - radius * 2, height - radius * 2, width, height), 270, 360, None, brush)
 
    # center rectangle
    draw.rectangle((radius, radius, width - radius, height - radius), brush)
 
    # four edge rectangle
    draw.rectangle((radius, 0, width - radius, radius), brush)
    draw.rectangle((0, radius, radius, height - radius), brush)
    draw.rectangle((radius, height - radius, width - radius, height), brush)
    draw.rectangle((width - radius, radius, width, height - radius), brush)
    draw.flush()
 
    image = image.convert('RGBA')
    image.putalpha(mask)
    return image


def crop_image(image):
    """function to crop and resize the image"""
    width, height = image.size

    if width == height:
        newsize = (im_size, im_size)
    
        # resizing profile image
        image = image.resize(newsize)
        radius, height = image.size
        image = round_corner_jpg(image, radius / 2)
        return image
    offset = int(abs(height - width) / 2)
    if width > height:
        image = image.crop([offset, 0, width - offset, height])
    else:
        image = image.crop([0, offset, width, height - offset])

    newsize = (im_size, im_size)

    # resizing profile image
    image = image.resize(newsize)
    radius, height = image.size
    image = round_corner_jpg(image, radius / 2)
    return image


def generate_image(user_id, picture_url, language_id, name):
    """Function for generating image"""
    try:

        # images that needs to be imported from s3 on language basis
        if language_id == 165 and name[-1] != "s":
            if picture_url != DEFAULT_IMAGE:
                if len(name) < 8:
                    background_img = Image.open(urlopen(background_img_url))
                else:
                    background_img = Image.open(urlopen(background_img_url_long))
            else:
                if len(name) < 8:
                    background_img = Image.open(urlopen(background_img_url_no_pic))
                else:
                    background_img = Image.open(urlopen(background_img_url_no_pic_long))

        elif language_id == 165 and name[-1] == "s":
            if picture_url != DEFAULT_IMAGE:
                if len(name) < 9:
                    background_img = Image.open(urlopen(background_img_url))
                else:
                    background_img = Image.open(urlopen(background_img_url_long))
            else:
                if len(name) < 9:
                    background_img = Image.open(urlopen(background_img_url_no_pic))
                else:
                    background_img = Image.open(urlopen(background_img_url_no_pic_long))

        elif language_id == 245:
            if picture_url != DEFAULT_IMAGE:
                background_img = Image.open(urlopen(background_img_url_es))
            else:
                background_img = Image.open(urlopen(background_img_url_no_pic_es))

        profile_img = Image.open(urlopen(picture_url))
        profile_img = crop_image(profile_img)
        user_name = str(name).capitalize()
        name = name.lower()

        # adding profile image to background image 
        if picture_url != DEFAULT_IMAGE:
            resized_profile_img = profile_img

            # formatting the background image
            formatted_img = background_img.copy()

            # pasting the profile image on the background image by formatting using mask image at the particular position
            formatted_img.paste(resized_profile_img, (100, 140), resized_profile_img)

            # defining the font to be included for text
            fnt = ImageFont.truetype(text_1_font, text_1_size)
        else:
            # formatting the background image
            formatted_img = background_img.copy()
            
            # defining the font to be included for text
            fnt = ImageFont.truetype(text_1_font, text_1_size)

        # adding the similarity score to the text
        if language_id == 165 and (name[-1]) != "s":

            if picture_url != DEFAULT_IMAGE:
                if len(user_name) < 8:
                    text = upper_text.format(user_name)
                else:
                    text = upper_text_long.format(user_name)

            elif picture_url == DEFAULT_IMAGE:
                if len(user_name) < 8:
                    text = upper_text.format(user_name)
                else:
                    text = upper_text_long.format(user_name)

        elif language_id == 165 and name[-1] == "s":

            if picture_url != DEFAULT_IMAGE:
                if len(user_name) < 9:
                    text = upper_text_no_s.format(user_name)
                else:
                    text = upper_text_long_no_s.format(user_name)

            elif picture_url == DEFAULT_IMAGE:
                if len(user_name) < 9:
                    text = upper_text_no_s.format(user_name)
                else:
                    text = upper_text_long_no_s.format(user_name)

        elif language_id == 245:

            text = upper_text_es.format(user_name)

      

        # creating the image draw object to put the text on formatted image
        d = ImageDraw.Draw(formatted_img)

        t_w, t_h = d.multiline_textsize(text, font=fnt)
        # Center Width
        c_l = (1200 - t_w - 505) / 2

        # writing the text on the formatted image at the required position using multiline text method on language basis
        if language_id == 165 and picture_url == DEFAULT_IMAGE:
            if len(name) < 8:
                d.multiline_text((479 + c_l, 242), text, fill='#333333', font=fnt, align="center")
            else:
                d.multiline_text((479 + c_l, 211), text, fill='#333333', font=fnt, align="center")

        elif language_id == 165 and picture_url != DEFAULT_IMAGE:
            if len(name) < 8:
                d.multiline_text((479 + c_l, 242), text, fill='#333333', font=fnt, align="center")
            else:
                d.multiline_text((479 + c_l, 212), text, fill='#333333', font=fnt, align="center")


        elif language_id == 245:
            d.multiline_text((480 + c_l, 275), text, fill="#333333", font=fnt, align="center")


        # calling the function to upload the image to s3 bucket
        return upload_image(formatted_img, user_id, language_id)
        
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['IMAGE_GENERATION_STATUS'])


def handler(event, context):
    """Function to handle the request for generating similarity image on Profiles Page"""
    logger.info("TEST")
    global mystery_unlock_user_count
    try:
        # fetching user data from event json
        rid = event['rid']

    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'])

    try:
        # Making the DB connection
        cnx = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        try:
            # Query for getting the users required details
            checkQuery = "SELECT `id`, `user_id`, `picture_url`,`language_id`, cast(AES_DECRYPT(`firstname`, %s) as char) FROM `users` WHERE `id` = %s"
            cursor.execute(checkQuery, (key, int(rid)))

            # Dictionary for storing result data
            result_list = []
            # fetching the result from the cursor
            for result in cursor: result_list.append(result)

            if result_list[0][2] == None:
                # calling the generate_image function to generate the custom image
                image_url = generate_image(result_list[0][1], DEFAULT_IMAGE, result_list[0][3], result_list[0][4])
            else:
                # calling the generate_image function to generate the custom image
                image_url = generate_image(result_list[0][1], result_list[0][2], result_list[0][3], result_list[0][4])

            # Returning a json response to the request by using required data 
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
                'body': json.dumps({"image_url": image_url})
            }
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['QUERY_EXECUTION_STATUS'])
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'])


if __name__ == "__main__":
    handler()
