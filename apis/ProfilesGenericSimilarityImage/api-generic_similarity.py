"""API Module to be called asynchronously from Friends or Someone Else profile page when user is Logged In.

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
from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageChops, ImageOps
import aggdraw
import io
import boto3
from botocore.client import Config
from urllib.request import urlopen

message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('generic_similarity.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# images that needs to be imported from s3
background_img_url = environ.get('BACKGROUND_IMG_URL')
background_img_url_es = environ.get('BACKGROUND_IMG_URL_ES')
background_img_url_no_pic = environ.get('BACKGROUND_IMG_URL_NO_PIC')
background_img_url_no_pic_es = environ.get('BACKGROUND_IMG_URL_NO_PIC_ES')
side_text = environ.get('SIDE_TEXT')
side_text_es = environ.get('SIDE_TEXT_ES')
DEFAULT_IMAGE = environ.get('DEFAULT_IMAGE')

# variables that can be used for text and image formatting
im_size = int(environ.get('IMG_SIZE'))
upper_text = environ.get('UPPER_TEXT')
upper_text_no_s = environ.get('UPPER_TEXT_NO_S')
upper_text_es =  environ.get('UPPER_TEXT_ES')

middle_text = environ.get('MIDDLE_TEXT')
middle_text_es = environ.get('MIDDLE_TEXT_ES')

lower_text = environ.get('LOWER_TEXT')
lower_text_es = environ.get('LOWER_TEXT_ES')


text_1_font = environ.get('TEXT_1_FONT')
text_1_size = int(environ.get('TEXT_1_SIZE'))
text_2_font = environ.get('TEXT_2_FONT')
text_2_size = int(environ.get('TEXT_2_SIZE'))
text_3_font = environ.get('TEXT_3_FONT')
text_3_size = int(environ.get('TEXT_3_SIZE'))

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


def upload_image(data, user_id, friends_user_id, language_id):
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
        image_name = image_template.format(str(language_id), str(user_id), str(friends_user_id))
        # uploading object to s3 bucket
        response = S3.Object(BUCKET_NAME,SUB_FOLDER + image_name).put(Body=image_bytes, ACL='public-read-write')
        # returning the url of the image
        return BUCKET_URL + image_name
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['UPLOAD_STATUS'])

def round_corner_jpg(image, radius):
    """Function to generate round corner for image"""
    mask = Image.new('L', image.size)  # filled with black by default
    draw = aggdraw.Draw(mask)
    brush = aggdraw.Brush('white')
    width, height = mask.size
    # upper-left corne
    # draw.pieslice((0,0,radius*2, radius*2), 90, 180, None, brush)
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


def generate_image(user_data, friends_data, similarity_score, language_id):
    """Function for generating image"""
    try:
        # converting simalrityscore value from intiger to string
        similarity_score = str(similarity_score)

        # images that needs to be imported from s3
        profile_img = Image.open(urlopen(user_data[1]))
        friends_img = Image.open(urlopen(friends_data[1]))

        side_text_pic = Image.open(urlopen(side_text)).convert("RGBA")
        side_text_es_pic = Image.open(urlopen(side_text_es)).convert("RGBA")

        # user and frinds name values
        user_name = str(user_data[2]).capitalize()
        friend_name = str(friends_data[2]).capitalize()

        # profile_img = crop_image(profile_img)
        profile_img = crop_image(profile_img)
        friends_img = crop_image(friends_img)


        # images that needs to be imported from s3 on language basis
        if language_id == 165:
            if profile_img == friends_img:
                background_img = Image.open(urlopen(background_img_url_no_pic))
            else:
                background_img = Image.open(urlopen(background_img_url))

        elif language_id == 245:
            if profile_img == friends_img:
                background_img = Image.open(urlopen(background_img_url_no_pic_es))
            else:
                background_img = Image.open(urlopen(background_img_url_es))

        # masking image for generating the circular profile image
        mask_im = Image.new("L", (im_size, im_size), 0)

        # resizing profile image
        resized_profile_img = profile_img.convert("RGBA")


        # resizing friends image
        resized_friends_img = friends_img.convert("RGBA")


        # formatting the background image
        formatted_img = background_img.copy()

        # defining the font to be included for text
        fnt = ImageFont.truetype(text_1_font, text_1_size)

        # getting the font type for the blue portion in the last line
        fnt_2 = ImageFont.truetype(text_2_font, text_2_size)

        # getting the font type for the background percent impression
        fnt_3 = ImageFont.truetype(text_3_font, text_3_size)


        # adding the similarity score to the text and eliminating 's
        if friend_name[-1].lower() == "s":
            text_u = upper_text_no_s.format(user_name, friend_name)
        else:
            text_u = upper_text.format(user_name, friend_name)

        text_m_es = middle_text_es.format(user_name, friend_name)

        text_l = lower_text.format(similarity_score)

        # creating the image draw object to put the text on formatted image
        d = ImageDraw.Draw(formatted_img)

        # adding the text to background on basis of language
        # if user has no profile picture with default langugae english
        if language_id == 165 and friends_img == profile_img:
            ss = similarity_score+"%"
            t_w_ss, t_h_ss = d.multiline_textsize(ss, font=fnt_3)
            c_l_ss = (1200 - t_w_ss)/2
            t_w, t_h = d.multiline_textsize(text_u, font=fnt)
            c_l = (1142 - t_w) / 2
            d.text((c_l_ss, 115), ss, fill="#eeece9", font=fnt_3, align="center")
            d.text((c_l, 213), text_u, fill="#333333", font=fnt, align="center")
            d.text((250, 275), middle_text, fill="#333333", font=fnt, align="center")
            d.text((470, 352), text_l, fill="#333333", font=fnt, align="center")
            d.text((520, 334), similarity_score + "%.", fill="#09A1BC", font=fnt_2, align="center")
            formatted_img.paste(side_text_pic, (917, 413), side_text_pic)

        # if user has no profile picture and default langugae spanish
        elif language_id == 245 and friends_img == profile_img:
            ss = similarity_score+"%"
            t_w_ss, t_h_ss = d.multiline_textsize(ss, font=fnt_3)
            c_l_ss = (1200 - t_w_ss)/2
            t_w, t_h = d.multiline_textsize(text_m_es, font=fnt)
            c_l = (1142 - t_w) / 2
            d.text((c_l_ss, 115), ss, fill="#eeece9", font=fnt_3, align="center")
            d.text((380, 213), upper_text_es, fill="#333333", font=fnt, align="center")
            d.text((c_l, 275), text_m_es, fill="#333333", font=fnt, align="center")
            d.text((460, 352), lower_text_es, fill="#333333", font=fnt, align="center")
            d.text((530, 334), similarity_score + "%.", fill="#09A1BC", font=fnt_2, align="center")
            formatted_img.paste(side_text_es_pic, (917, 400),side_text_es_pic)



        # if user has profile picture with default langugae english
        elif language_id == 165:
            t_w_ss, t_h_ss = d.multiline_textsize(similarity_score, font=fnt_3)
            c_l_ss = 675 - t_w_ss - 15
            t_w, t_h = d.multiline_textsize(text_u, font=fnt)
            c_l = (1142 - t_w) / 2
            d.text((c_l_ss, 115), similarity_score, fill="#eeece9", font=fnt_3, align="center")
            d.text((c_l, 44), text_u, fill="#333333", font=fnt, align="center")
            d.text((470, 184), text_l, fill="#333333", font=fnt, align="center")
            d.text((520, 166), similarity_score + "%.", fill="#09A1BC", font=fnt_2, align="center")


        # if user has no profile picture with default langugae spanish
        elif language_id == 245:
            t_w_ss, t_h_ss = d.multiline_textsize(similarity_score, font=fnt_3)
            c_l_ss = 675 - t_w_ss - 15
            t_w, t_h = d.multiline_textsize(text_m_es, font=fnt)
            c_l = (1142 - t_w) / 2
            d.text((c_l_ss, 115), similarity_score, fill="#eeece9", font=fnt_3, align="center")
            d.text((c_l, 106), text_m_es, fill="#333333", font=fnt, align="center")
            d.text((460, 184), lower_text_es, fill="#333333", font=fnt, align="center")
            d.text((530, 165), similarity_score + "%.", fill="#09A1BC", font=fnt_2, align="center")


        # pasting the profile image on the background image by formatting using mask image
        if friends_img == DEFAULT_IMAGE and friends_img != profile_img:
            formatted_img.paste(DEFAULT_IMAGE, (554, 261),DEFAULT_IMAGE)
        elif friends_img != profile_img:
            formatted_img.paste(resized_friends_img, (554, 261),resized_friends_img)

        # pasting the friends profile image on the background image by formatting using mask image
        if profile_img == DEFAULT_IMAGE and friends_img != profile_img:
            formatted_img.paste(DEFAULT_IMAGE, (324, 261),DEFAULT_IMAGE)
        elif friends_img != profile_img:
            formatted_img.paste(resized_profile_img, (324, 261),resized_profile_img)

        # calling the function to upload the image to s3 bucket
        return upload_image(formatted_img, user_data[3], friends_data[3], language_id)

    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['IMAGE_GENERATION_STATUS'])

def handler(event, context):
    """Function to handle the request for generating similarity image on Friends Profile Similarity Page"""
    # logger.info(event)
    global mystery_unlock_user_count
    try:
        # fetching user data from event json

        # User Rid
        users_rid = event['rid']

        # Friends Rid
        friends_rid = event['friends_rid']

        # Similarity Score
        similarity_score = event['similarity_score']

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
            # Query for getting the user and users friends required details 
            checkQuery = "SELECT `id`, `user_id`, `picture_url`, cast(AES_DECRYPT(`firstname`, %s) as char), `language_id` FROM `users` WHERE `id` in (%s, %s)"
            cursor.execute(checkQuery, (key, int(users_rid), int(friends_rid)))

            # Dictionary for storing result data
            result_dict = {}
            
            # fetching the result from the cursor
            for record in cursor:
                # getting result dict for both the users
                if record[2] == None:
                    result_dict[record[0]] = [record[0], DEFAULT_IMAGE, record[3], record[1], record[4]]
                else:
                    result_dict[record[0]] = [record[0], record[2], record[3], record[1], record[4]]
            
            # fetching language_code
            user_language_code = result_dict[users_rid][4]
            
            # calling the generate_image function to generate the custom image
            image_url = generate_image(result_dict[users_rid], result_dict[friends_rid], similarity_score, user_language_code)
						
            # Returning a json response to the request by using required data 
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
                'body': json.dumps({"status": "success", "url": image_url})
            }

        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['QUERY_EXECUTION_STATUS'])
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'])


