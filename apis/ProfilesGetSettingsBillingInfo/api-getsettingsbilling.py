"""
API for getting Hosted Page for managing payment sources and transaction history for user to make product purchase.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching data from request
- creating chargebee customer for an user if he does not have any customer_id associated with the user
- fetching transaction history of the customer
- getting a manage payment sources hosted page for current user
- Returning the JSON response with success status code with the required data
"""

import chargebee
import pymysql
import jwt
import logging
import json
from os import environ
import traceback
import configparser
from datetime import datetime
from pyDes import *

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('getsettingsbilling.properties', encoding = "ISO-8859-1")

# getting message variable
message_by_language = "165_MESSAGES"

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')
INVOICE_SECRET = environ.get('INVOICE_SECRET')
tnx_status = environ.get('TNX_STATUS')

# Environment required for chargebee
SITE_KEY = environ.get('SITE_KEY')
SITE_URL = environ.get('SITE_URL')

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

def handler(event,context):
    """Function to handle the request for Get Settings Billing API"""
    global message_by_language
    logger.info(event)
    try:
        # getting variable from request
        auth_token = event['headers']['Authorization']

        # configuring chargebee object
        chargebee.configure(SITE_KEY,SITE_URL)
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
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        try:
            # getting chargebee_id of the user
            selectionQuery = "SELECT `language_id`, `customer_id`, CAST(AES_DECRYPT(`firstname`,%s) AS CHAR), CAST(AES_DECRYPT(`lastname`,%s) AS CHAR), CAST(AES_DECRYPT(`primary_email`,%s) AS CHAR) FROM `users` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (key, key, key, rid))

            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)

            logger.info(result_list)

            # getting the data from the result_list
            language_id = result_list[0][0]
            message_by_language = str(language_id) + "_MESSAGES"
            customer_id = result_list[0][1]
            first_name = result_list[0][2]
            last_name = result_list[0][3]
            email = result_list[0][4]


            selectionQuery = "SELECT `code` FROM `language` WHERE `id`=%s"
            cursor.execute(selectionQuery, (int(language_id)))
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            language_code = result_list[0][0]

            if customer_id == None:
                # creating a Chargebee customer for current user
                result = chargebee.Customer.create({
                                                   "first_name" : first_name,
                                                   "last_name" : last_name,
                                                   "email" : email,
                                                   "locale": language_code
                                                   })
                # getting customer id of the customer created above
                customer_id = str(result.customer.id)

                # inserting customer_id into users account
                updationQuery = "UPDATE `users` SET `customer_id` = %s WHERE `id`=%s"
                # Executing the Query
                cursor.execute(updationQuery, (customer_id, rid))

            # getting hosted page for managing payment sources data
            result = chargebee.HostedPage.manage_payment_sources({
                            "customer" : { "id" : customer_id }
                        })

            # getting transaction list associated with a customer from chargebee
            transaction_list = chargebee.Transaction.list({
                            "customer_id[is]" : customer_id,
                            "limit":100,
                            "sort_by[desc]" : "date"
                        })

            transactions_data = {}

            # Iterating all transactions which we get from the above call
            d = []

            for entry in transaction_list:
                # getting transaction from transaction list in converting it to our required format
                transaction = json.loads(str(entry.transaction))
                if transaction["status"] == tnx_status:
                    
                    try:
                        # getting data from the current transaction and arranging it in required order
                        data = { "status" : transaction['linked_invoices'][0]['invoice_status'].capitalize(), "date" : transaction['date'], "invoice_id":(triple_des(INVOICE_SECRET).encrypt(transaction['linked_invoices'][0]['invoice_id'], padmode=2)).hex(), "amount" : "$" + str(round(transaction['amount']/100))}
                        transactions_data[transaction['subscription_id']] = data
                        d.append(transactions_data[transaction['subscription_id']])
                    except:
                        if transaction['type'].capitalize() == "Refund":
                            status = "Refunded"
                            pass
                        # getting data from the current transaction and arranging it in required order
                        data = {"status": status, "date": transaction['date'], "invoice_id": (triple_des(INVOICE_SECRET).encrypt(transaction['linked_credit_notes'][0]['cn_id'], padmode=2)).hex(), "amount" : "$" + str(round(transaction['amount']/100)), "subscription_id": transaction["subscription_id"]}
                        transactions_data[transaction['subscription_id']] = data
                        d.append(transactions_data[transaction['subscription_id']])


            ans = []
            new_data = []
            subscription_list = list(transactions_data.keys())

            if subscription_list != []:

                # getting the subscription list from chargebee according to the above transactions
                subscriptions_list = chargebee.Subscription.list({
                                "id[in]" : list(transactions_data.keys()),
                                "limit" : 100,
                                "sort_by[desc]" : "created_at"
                            })

                plans_data = {}
                plans_id_list = []

                # getting plans associated with the subscriptions
                for entry in subscriptions_list:
                    # fetching subscription from subscription list
                    subscription = json.loads(str(entry))['subscription']

                    # fetching plan id associated with the subscription
                    plans_data[subscription['id']] = subscription['plan_id']
                    plans_id_list.append(subscription['plan_id'])

                plan_list=",".join(["\"" + i + "\"" for i in list(set(plans_id_list))])

                logger.info(plan_list)

                # inserting customer_id into users account
                if int(language_id) == 165:
                    selectionQuery = "SELECT `plan_id`, `product_name` FROM `products` WHERE `plan_id` IN ( " + plan_list + " )"
                else:
                    selectionQuery = "SELECT `plan_id`, `product_name` FROM `products_translations` WHERE `plan_id` IN ( " + plan_list + " )"
                # Executing the Query
                cursor.execute(selectionQuery)

                plan_names_list = {}

                # iterating over all the plans in plan list
                for i in cursor:
                    # fetching plan name associated with plan id
                    plan_names_list[i[0]] = i[1]


                logger.info(plan_names_list)

                # iterating over plans associated with the subscriptions

                for i in list(plans_data.keys()):
                    # adding product name according to the account
                    transactions_data[i]['product_name'] = plan_names_list[plans_data[i]]
                    # appending transaction to ans list
                    ans.append(transactions_data[i])

                

                # adding refunded data in billing history
                if len(ans) != len(d):
                    for x in d:
                        if x['status'] == "Refunded":
                            if int(language_id) == 165: 
                                selectionQuery = "SELECT `product_name` FROM `products` WHERE `plan_id`=%s"
                            else:
                                selectionQuery = "SELECT `product_name` FROM `products_translations` WHERE `plan_id`=%s"
                            cursor.execute(selectionQuery, (plans_data[x["subscription_id"]]))
                            x["product_name"] = cursor.fetchone()[0]
                            del x["subscription_id"]
                            new_data.append(x)
                        else:
                            new_data.append(x)
                else:
                    new_data = ans
            
            # returning the success json with the required data
            return  {
                        'statusCode': 200,
                        'headers': {
                                    'Access-Control-Allow-Origin': '*',
                                    'Access-Control-Allow-Credentials': 'true'
                                   },
                        'body': json.dumps({"site":SITE_URL ,"hosted_page" : json.loads(str(result.hosted_page)),"billing_history":new_data})
                    }
        except:
            logger.info(traceback.format_exc())
            # If there is any error in above operations, logging the error
            return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
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
