"""
API for generating User Profile Report

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. get_dataframe_list(): getting list of dataframes from the database
4. retrieve_users_report(): retrieve users data from the database
5. interpolate_users_data(): getting users data and interpolating age and gender
6. predicted_score_across_clusters(): getting predicted scores across the cluster for each user
7. calculate_Z_score(): Function to calculate the z-score
8. percentile_score_across_clusters(): Getting percentile score across clusters for each user
9. assign_user_to_style(): Function for assigning users to each style
10. processing_to_assign_user_to_style(): Pre and post processing the function call assign_user_to_style
11. report_generation(): generating the interpersonality report for the users pair
12. make_interpersonality_report(): writing the report related to the user to database
13. handler(): Handling the incoming request with following steps
- Fetching the data from the database
- processing the data and then generating the interpersonality report for the users pair
- returning the success from the method

"""

import pandas as pd
import os
import pymysql
import numpy as np
import scipy.stats as stats
import math
import traceback
import configparser
import json
import logging

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('user_pair_interpersonality_report.properties')

message_by_language = os.environ.get('MESSAGES_LANGUAGE')

# Setting the number of input variables
INPUT_VAR_NUMBER = int(os.environ.get('INPUT_VAR_NUMBER'))

# Setting a parameter to choose either of the input files
# Set 1 : To chosse input file 'cluster_personality_types_output.csv'
# Set 2 : To choose input file 'Personalty_type_percentile_scores.csv'
CLUSTER_PT_OUTPUT_OR_PT_PERCENTILE_SCORES = int(os.environ.get('CLUSTER_PT_OUTPUT_OR_PT_PERCENTILE_SCORES'))

# Defining DB paramaters
DB_USER = os.environ.get('DBUSER')
PASSWD = os.environ.get('DBPASSWORD')
HOST = os.environ.get('ENDPOINT')
DB_NAME = os.environ.get('DATABASE')
DB_PORT = os.environ.get('PORT')

# Defining DB paramaters for database
PROFILES_DB_USER = os.environ.get('PROFILES_DBUSER')
PROFILES_PASSWD = os.environ.get('PROFILES_DBPASSWORD')
PROFILES_HOST = os.environ.get('PROFILES_ENDPOINT')
PROFILES_DB_NAME = os.environ.get('PROFILES_DATABASE')
PROFILES_DB_PORT = os.environ.get('PROFILES_PORT')

# secret keys for data encryption
key = os.environ.get('DB_ENCRYPTION_KEY')

# Files from which data is to be fetched
MEAN_SD_REG_FUN = os.environ.get('MEAN_SD_REG_FUN')
PERCENTILE_SCORES = os.environ.get('PERCENTILE_SCORES')
INTERPERSONAL_CONTENTS = os.environ.get('INTERPERSONAL_CONTENTS')

# tables from which data is to be fetched
INPUT_TABLE_NAME = os.environ.get('INPUT_TABLE_NAME')
USER_TABLE_NAME = os.environ.get('USER_TABLE_NAME')
USER_INTERPERSONAL_REPORT = os.environ.get('USER_INTERPERSONAL_REPORT')

# Interpolating 'age' in the range of -1 to +1 based on the min and max age as 0 and 100 
y1 = int(os.environ.get('Y1_GENDER'))
y2 = int(os.environ.get('Y2_GENDER'))
x1 = int(os.environ.get('X1_AGE'))
x2 = int(os.environ.get('X2_AGE'))
theme_order = os.environ.get('THEME_ORDER')

# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
# Setting the log level to INFO
logger.setLevel(logging.INFO)

logger.info("Cold start complete.") 

def make_connection(hostname, database_user, database_password, database_name, database_port):
    """Function to make the database connection."""
    
    # Create a connection with MySQL database.
    return pymysql.connect(host=hostname, user=database_user, passwd=database_password,
                           db=database_name, port=int(database_port))
                           
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

def get_dataframe_list(theme_num, csv_name):
    """Function for reading multiple files and returning the result list for data in all files"""
    
    # creating the list of filenames with the current path that needed to be fetched by using theme no.
    filenames = [ csv_name + str(i) + ".csv" for i in theme_num]
    result_list = []
    
    # Iterating over all the file 
    for i in range(0, len(filenames)):
        # reading each csv and appending it to the result_list
        result_list.append(pd.read_csv(filenames[i]))
        
    # returning the result_list containing data of all the files
    return result_list

def retrieve_users_report(user_ids, db_connection):
    """Retrieve the user's input data from the MySQL database"""
    
    # constructing the query for fetching data from the tables
    query = "SELECT * FROM {0} input, {1} users WHERE input.user_id = users.user_id AND users.user_id in (%(first_user)s, %(second_user)s) ORDER BY FIELD(users.user_id, %(order_first)s, %(order_second)s)".format(INPUT_TABLE_NAME, USER_TABLE_NAME)
    
    # Executing the query for fetching data from database
    users_report_df = pd.read_sql(query, db_connection, params={"first_user":user_ids[0], "second_user":user_ids[1], "order_first":user_ids[0], "order_second":user_ids[1]})
        
    logger.info(users_report_df)
    
    # Removing the user_id column from the dataset (or removing duplicate column)
    users_report_df = users_report_df.loc[:,~users_report_df.columns.duplicated()]
    
    # Removing the duplicated rows from the dataframe
    users_report_df = users_report_df.loc[~(users_report_df.user_id.duplicated())]
    
    # if the user are
    if user_ids[0] == user_ids[1]:
        users_report_df = pd.concat([users_report_df, users_report_df])
    
    # resetting the index of the dataframe
    users_report_df = users_report_df.reset_index(drop=True)
    
    # Removing unwanted column from the data frame
    users_report_df.drop(["id","timestamp"], axis = 1, inplace = True)
    
    # returning the user report dataframe
    return users_report_df
    
def calculate_similarity_score(users_report_df):
    """"Finding correlation between two dataframes"""
    
    # Removing unwanted column from the data frame
    users_report_df = users_report_df.drop(["age", "gender"], axis = 1)
    
    # making each dataframe as a dataframe to correlate it
    first_user_trait_scores_df = pd.DataFrame([users_report_df.iloc[0,1:]])
    second_user_trait_scores_df = pd.DataFrame([users_report_df.iloc[1,1:]])
    
    # reindexing both the dataframe or preprocessing it before correlating it
    first_user_trait_scores_df = first_user_trait_scores_df.reset_index(drop=True)
    second_user_trait_scores_df = second_user_trait_scores_df.reset_index(drop=True)

    # To find the correlation among the 
    # columns of df1 and df2 along the row axis 
    correlation_value = first_user_trait_scores_df.corrwith(second_user_trait_scores_df, axis=1)
    
    # calculating the similarity_score by using the correlation value
    similarity_score = int(round((((correlation_value[0] + 1)/2))*100))
    
    return similarity_score

def interpolate_users_data(users_report_df):
    """Interpolating the users input data"""
    
    # Assigning 1 to all males
    users_report_df.loc[users_report_df.gender == "male", "gender"] = 1
    
    # Assigning -1 to all female
    users_report_df.loc[users_report_df.gender == "female", "gender"] = -1
    
    # Interpolating age in dataset
    users_report_df.age = y1 + (((users_report_df.age - x1) / (x2-x1)) * (y2-y1))
    
    # returning the response
    return users_report_df

def predicted_score_across_clusters(list_mean_sd_reg_fun, users_report_df, theme_num):
    """Users predicted scores across the clusters for each theme"""
    
    # list of users ids from dataframe
    users_to_generate__report = users_report_df['user_id'].tolist()
    
    # count no. of users
    user_count_for_report = len(users_to_generate__report)
    
    # Defining a list to store data frames of users predicted scores (across 
    # all clusters) for each theme
    user_clust_predscore_df_list = list(range(0, len(theme_num)))
    
    
    # Iterate over each theme means_sd_reg_fun list
    for i in range(0, len(theme_num)):
        
        # Extracting the mean_sd_coef data for the theme from list
        mean_sd_coef_df = list_mean_sd_reg_fun[i]
        
        # Extracting number of clusters in the dataframe 'mean_sd_coef_df'
        cluster_of_cluster_count = len(mean_sd_coef_df['cluster'])
        
        # Defining a data frame to store users predicted scores for each cluster
        user_clust_predscore_df = pd.DataFrame(np.full((user_count_for_report,cluster_of_cluster_count), np.nan))
        
        # Iterate over each user
        for j in range(0,user_count_for_report):
            
            # Exctracting users data for input variables
            user_input_data = (users_report_df.iloc[j,1:]).to_list()
            
            # Getting the user's predicted score for each cluster
            results1 = [[n*m for n,m in zip(user_input_data, (mean_sd_coef_df.iloc[k,4:]).to_list())] 
                                                        for k in range(0, cluster_of_cluster_count)]
            
            coeff_users_prod = pd.DataFrame(results1)
            
            user_pred_score_clusters = [n+m for n,m in zip((coeff_users_prod.sum(axis=1)).to_list(), 
                                                         (mean_sd_coef_df['intercept']).to_list())] 
        
            user_clust_predscore_df.loc[j] = user_pred_score_clusters
            
        user_clust_predscore_df_list[i] = user_clust_predscore_df
    
    # returning the resulted list containing users predicted scores across the clusters for each theme
    return user_clust_predscore_df_list, user_count_for_report

def calculate_Z_score(fuser_score, fmean, fsd):
    """Function to calculate z score"""
    
    # using the scipy library calculating the z-score/percentile in file
    zscore_pct = stats.norm.cdf(fuser_score, loc=fmean, scale=fsd)
    
    # returning the zscore
    return zscore_pct

def percentile_score_across_clusters(user_clust_predscore_df_list, list_mean_sd_reg_fun ,theme_num, user_count_for_report):
    """Generating the users percentile/z-score for each cluster of cluster"""
    # Defining a list to store data frames of users percentile scores (across 
    # all clusters) for each theme
    user_clust_zscore_df_list = list(range(0, len(theme_num)))
    
    # Iterate over each themes for PT_percentile_score list
    for i in range(0, len(theme_num)):
        
        # Extracting the mean_sd_coef data for the theme from the list
        mean_sd_coef_df = list_mean_sd_reg_fun[i]
        
        # Extracting the PT_percentile_score data for the theme from the list
        user_clust_predscore_df = user_clust_predscore_df_list[i]
        
        # Extracting number of clusters in the dataframe 'mean_sd_coef_df'
        cluster_of_cluster_count = len(mean_sd_coef_df['cluster'])
        
        # Defining the data frame to store z-score/percentile of the all users across all
        # clusters
        user_clust_zscore_df = pd.DataFrame(np.full((user_count_for_report, cluster_of_cluster_count), np.nan))
        
        for j in range(0, user_count_for_report):
            # Itrating over each cluster
            zscore_pct = [calculate_Z_score(user_clust_predscore_df.iloc[j,k], mean_sd_coef_df['mean'].iloc[k], 
                            mean_sd_coef_df['stdev'].iloc[k]) for k in range(0, cluster_of_cluster_count)]
            
            # inserting the resulted zscore_pct dataframe into user_clust_zscore_df_list
            user_clust_zscore_df.loc[j] = zscore_pct
            
        # inserting the resulted zscore_pct dataframe into user_clust_zscore_df_list
        user_clust_zscore_df_list[i] = user_clust_zscore_df
    
    # returning the resulted list
    return user_clust_zscore_df_list

def assign_user_to_style(f_styles_percentile_score_df,f_user_percentile_score):
    """Function for Assigning users to each styles"""
    
    # Getting the square root of sum of squared values of user percentile score
    self_product_row = math.sqrt((f_user_percentile_score**2).values.sum())
    
    # Getting the count of styles in the theme
    count_styles_in_theme = len(f_styles_percentile_score_df.axes[0])
    
    # Defining vector to store dihedral angle between the users percentile score and 
    # style percentile score 
    dihedral_angles_vec = [0]*count_styles_in_theme
    
    for i in range(0,count_styles_in_theme):
        # Getting the style's percentile scores   
        style_score = f_styles_percentile_score_df.iloc[i,1:]
        
        # Getting the square root of sum of squared values style percentiel score
        self_product_column = math.sqrt((style_score**2).values.sum())
        
        # Multiplying corresponding coefficients of both the vector
        sum_of_dot_product = sum([n*m for n,m in zip(f_user_percentile_score.iloc[0,:], style_score)])
        
        # Dihedral formula for calculation the value of cos(x)
        cos_x = sum_of_dot_product / (self_product_row * self_product_column)
        
        # Getting the value of angle in degree
        dihedral_angle = (math.acos(cos_x)*180)/math.pi
        
        # Assigning the angle to the dihedral_angles_vec
        dihedral_angles_vec[i] = dihedral_angle
    
    # Getting the minimum dihedral angle
    min_angle = min(dihedral_angles_vec)
    
    # Getting the index of style
    min_index = dihedral_angles_vec.index(min_angle)
    
    # returning the min_index of the min_angle and min_angle
    return min_index,min_angle


def processing_to_assign_user_to_style(user_clust_zscore_df_list, list_PT_percentile_score, theme_num, user_count_for_report):
    """Pre and post processing for function call"""
    
    # Defining a dataframe to store the details about user assignment to style and angle
    user_theme_style_angle_df = pd.DataFrame(np.full((0,4), np.nan))
    
    # Defining a dataframe to store the details about user assignment to style and angle
    user_theme_style_angle_df_horizontal = pd.DataFrame()
    
    # Iterating over each user
    for i in range(0, user_count_for_report):
        
        # Defining a dataframe to store user's style assignment across the themes
        style_assignments_user_df = pd.DataFrame(np.full((0,3), np.nan))
        
        # Iterate over each theme
        for j in range(0, len(theme_num)):
            # Getting the users zscore across the cluster for the theme
            users_percentile_score_df = user_clust_zscore_df_list[j]
            
            # Getting the user zscore: index by 0 row, so it will fetch only the first row at a time since the is only one user
            # theme df interate using the j for loop, adjusting by 0.5
            # iterating over the len of the users columns
            user_percentile_score = pd.DataFrame([users_percentile_score_df.iloc[i,:]]) - 0.5
            
            # Getting the data frame of percentile score of theme-style
            styles_percentile_score_df = list_PT_percentile_score[j]
            
            # creating a series that could assign the value to subtract from each column
            subtract_from_columns = pd.Series([0 if i=="personality_type" else 0.5 for i in list(styles_percentile_score_df.columns) ], index =[i for i in list(styles_percentile_score_df.columns)])
            
            # subtracting the -0.5 from all the attributes and skipping personality_type column
            styles_percentile_score_df = styles_percentile_score_df.subtract(subtract_from_columns, axis=1)
            
            # Function call to assign user to one style in the theme
            style_angle = assign_user_to_style(styles_percentile_score_df, user_percentile_score)
            
            # Making the row with required details
            details_row = [i+1, j+1, style_angle[0]+1, style_angle[1]]
            
            # making a row to store list for style assignment
            row_for_style_assignment = [i+1, style_angle[0]+1, style_angle[1]]
            
            # Binding the row to the data frame 'user_theme_style_angle_df' 
            user_theme_style_angle_df = pd.concat([user_theme_style_angle_df, pd.DataFrame([details_row])])
            
            # Binding the row 'row_for_style_assignment' to the dataframe 'style_assignments_user_df'
            style_assignments_user_df = pd.concat([style_assignments_user_df, pd.DataFrame([row_for_style_assignment])]) 
            
        # Assigning column names to the dataframe
        style_assignments_user_df.columns = ["user","style","angle"]
        
        # if there is no row in the dataframe
        if len(user_theme_style_angle_df_horizontal.axes[0]) != 0:
            # Assign actual theme number in the theme column
            user_theme_style_angle_df_horizontal = pd.concat([user_theme_style_angle_df_horizontal, style_assignments_user_df], axis=1)
        else:
            # assigning the dataframe style_assignments_user_df to user_theme_style_angle_df_horizontal
            user_theme_style_angle_df_horizontal = style_assignments_user_df
        
    # assigning the column names to the dataframe user_theme_style_angle_df
    user_theme_style_angle_df.columns = ["user", "theme", "style", "angle"]
    
    # reindexing the dataframe user_theme_style_angle_df_horizontal
    user_theme_style_angle_df_horizontal = user_theme_style_angle_df_horizontal.reset_index(drop=True)
    
    # reindexing the dataframe user_theme_style_angle_df
    user_theme_style_angle_df = user_theme_style_angle_df.reset_index(drop=True)
    
    # returning the resulted theme style angle dataframe
    return user_theme_style_angle_df, user_theme_style_angle_df_horizontal

def report_generation(theme_num, user_ids, users_report_df, user_theme_style_angle_df, report_content_df):
    """Generation of interpersonal report for user pair"""
    # Defining dataframe to store interpersonal report for user's pair
    pair_report_df = pd.DataFrame(np.full((len(theme_num), 5), np.nan))
    
    # creating a list with empty strings in it
    all_content_for_user_1 = [""]*len(theme_num)
    
    # Iterating over pair of the of users
    for i in range(0, len(user_ids)):
        # Fetching the user 1 gender
        user_1_gender = (users_report_df['gender']).iloc[i]
        
        # Iterate over each theme
        for j in range(0, len(theme_num)):
            
            # Fetch the style number in the theme
            style_num = user_theme_style_angle_df.loc[(user_theme_style_angle_df['user']==i+1) & (user_theme_style_angle_df['theme']==j+1)]['style']
            
            # Making of theme_style_number for comparing in report_content_df
            theme_style_num = [str(int(theme_num[j])) + str(int(i)).zfill(2) for i in style_num.to_list()]
            
            content_for_user = ""
            
            # Fetching the content of the style (theme wise) for user as per gender
            # User 1 and male
            if i==0 and user_1_gender==1 :
                # finding the rows in report_content_df dataframe which have the particular theme_style_num
                content_for_user =(report_content_df.loc[report_content_df['Number']==int(theme_style_num[0])])
                
                # if there is any content than moving forward
                if len(content_for_user) > 0:
                    # assigning content to the user4
                    content_for_user = content_for_user.iloc[0,4]
            
            # User 1 and female
            if i==0 and user_1_gender==-1:
                # finding the rows in report_content_df dataframe which have the particular theme_style_num
                content_for_user = (report_content_df.loc[report_content_df['Number']==int(theme_style_num[0])])
                
                # if there is any content than moving forward
                if len(content_for_user) > 0:
                    # assigning content to the user4
                    content_for_user = content_for_user.iloc[0,5]
                
            # User 2 and male
            if i==1 and user_1_gender==1:
                # finding the rows in report_content_df dataframe which have the particular theme_style_num
                content_for_user = (report_content_df.loc[report_content_df['Number']==int(theme_style_num[0])])
                
                # if there is any content than moving forward
                if len(content_for_user) > 0:
                    # assigning content to the user4
                    content_for_user = content_for_user.iloc[0,6]
                
            # User 2 and female
            if i==1 and user_1_gender==-1:
                # finding the rows in report_content_df dataframe which have the particular theme_style_num
                content_for_user = (report_content_df.loc[report_content_df['Number']==int(theme_style_num[0])])
                
                # if there is any content than moving forward
                if len(content_for_user) > 0:
                    # assigning content to the user4
                    content_for_user = content_for_user.iloc[0,7]
                
            # If there is no content for the user than simply assigning the string
            if len(content_for_user)==0:
                content_for_user = "text not available for the style"
            
            if i==0:
                # Assign theme number in column 1
                pair_report_df.iloc[j,0] = j+1
                
                # Assign user 1 style
                pair_report_df.iloc[j,1] = style_num.to_list()[0]
                
                # Assign content
                pair_report_df.iloc[j,3] = content_for_user
            else:
                # Assign user 1 style
                pair_report_df.iloc[j,2] = style_num.to_list()[0]
                
                # Assign content
                pair_report_df.iloc[j,4] = content_for_user
                
    # assigning columns to the report
    pair_report_df.columns = ["Theme",	"user_1_style", "user_2_style",	"user_1_text", "user_2_text"]
    
    # returning the final report dataframe
    return pair_report_df

def handler(event,context):
    """Main function for generating reports for each users"""
    
    try:
        # getting the user_id for which report need to be generated
        body = json.loads(event['body'])
        user_id_1 = body['user_id_1']
        user_id_2 = body['user_id_2']
        user_ids = tuple([user_id_1, user_id_2])
        language_id = int(body['language_id'])
        message_by_language = str(language_id) + "_MESSAGES"
    except:
        # returning the error when there is some error in above try block
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
        
    try:
        # 1) i) Reading the input file having mean, sd and regression function of clusters of clusters
        theme_num = list(range(1, 15))
        
        # getting list for the mean sd for each theme
        list_mean_sd_reg_fun = get_dataframe_list(theme_num, MEAN_SD_REG_FUN)
        
        # Intialize list to store the percentile score for each theme 
        list_PT_percentile_score = []
        
        # 1) ii) Loading the csv 'cluster_personality_types_output.csv' containing consumed
        #        personality types and cluster of cluster percentile scores
        if CLUSTER_PT_OUTPUT_OR_PT_PERCENTILE_SCORES == 2:
            # Intialize list to store the percentile score for each theme 
            list_PT_percentile_score = get_dataframe_list(theme_num, PERCENTILE_SCORES)
            
        # 1) iii) Loading the csv containing content for interpersonal report
        
        # Read the provided data input csv file having styles details for report section
        report_content_df = pd.read_csv(INTERPERSONAL_CONTENTS + ".csv")
        
        #2) Retrieve the user's input data from the MySQL database
        try:
            # Create a connection with MySQL database.
            db_connection = make_connection(HOST, DB_USER, PASSWD, DB_NAME, DB_PORT)
            
            try:
                # Retrieve the user's input data from the MySQL database
                users_report_df = retrieve_users_report(user_ids, db_connection)
                try:
                    # Fetching the gender of the user from the dataframe
                    user_1_gender = users_report_df['gender'].iloc[0]
                    user_2_gender = users_report_df['gender'].iloc[1]
                    
                    # Fetching the data of the user from the dataframe
                    users_report_df.iloc[0,1:]
                    users_report_df.iloc[1,1:]
                except:
                    # returning the error when there is some error in above try block
                    logger.error(traceback.format_exc())
                    return log_err(config[message_by_language]['USER_EXISTENCE'], 500)
            except:
                # returning the error when there is some error in above try block
                logger.error(traceback.format_exc()) 
                return log_err(config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
        except:
            # returning the error when there is some error in above try block
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)
        finally:
            db_connection.close()
        
        # 3) Interpolating the users input data
        users_report_df = interpolate_users_data(users_report_df)
        
        # 4) Users predicted score across the clusters for each theme
        user_clust_predscore_df_list, user_count_for_report = predicted_score_across_clusters(list_mean_sd_reg_fun, users_report_df, theme_num)
        
        # 5) Generating the users percentile/z-score for each cluster of cluster
        user_clust_zscore_df_list = percentile_score_across_clusters(user_clust_predscore_df_list, list_mean_sd_reg_fun ,theme_num, user_count_for_report)
        
        # 6 and 7) Pre and post processing and function call for Assigning users to each styles
        user_theme_style_angle_df, user_theme_style_angle_df_horizontal = processing_to_assign_user_to_style(user_clust_zscore_df_list, list_PT_percentile_score, theme_num, user_count_for_report)
        
        # 8) Generation of interpersonal report for user pair
        pair_report_df = report_generation(theme_num, user_ids, users_report_df, user_theme_style_angle_df, report_content_df)
        
        # 9) Finding the correlation value between two users data
        similarity_score = calculate_similarity_score(users_report_df)
        
        # 10) Ordering the theme
        ordering_list = [int(theme_no) for theme_no in theme_order.split(",")]
        
        # converting the pair_report dataframe to list
        pair_report_list = pair_report_df.values.tolist()
        user_1_list = []
        user_2_list = []
        comparison = []
        user_1_content = []
        user_2_content = []
        style_difference_scores = []
        theme = []
        
        #2) Retrieve the difference scores for each user pair
        try:
            # Create a connection with MySQL database for interpersonality.
            cnx = make_connection(PROFILES_HOST, PROFILES_DB_USER, PROFILES_PASSWD, PROFILES_DB_NAME, PROFILES_DB_PORT)
                                    
            # Getting the cursor from the DB connection to execute the queries
            cursor = cnx.cursor()

            if(user_1_gender=='female'):
                user_1_gender_id = 1
            else:
                user_1_gender_id = 0

            if(user_2_gender=='female'):
                user_2_gender_id = 1
            else:
                user_2_gender_id = 0 
            
            try:
                for pos in ordering_list:
                    
                    user_1_style_code = ''
                    user_2_style_code = ''

                    user_1_value = str(int(pair_report_list[pos-1][0])).zfill(2)+str(int(pair_report_list[pos-1][1])).zfill(2)
                    user_2_value = str(int(pair_report_list[pos-1][0])).zfill(2)+str(int(pair_report_list[pos-1][2])).zfill(2)
                    
                    # Query for getting the users required details
                    selectionQuery = "SELECT `difference_score` FROM `style_difference_scores` WHERE (`style1`=%s AND `style2`=%s) OR (`style1`=%s AND `style2`=%s)"
                    cursor.execute(selectionQuery, (int(user_1_value), int(user_2_value), int(user_2_value), int(user_1_value)))
                    
                    # Dictionary for storing result data
                    result_list = []
                    # fetching the result from the cursor
                    for result in cursor: result_list.append(result)

                    # Query for getting the theme name
                    selectionQuery = "SELECT `name` FROM `theme_names` WHERE `language_id`=%s AND `theme_id`=%s"                
                    cursor.execute(selectionQuery, (int(language_id), int(pos)))
                    
                    # Dictionary for storing result data
                    theme_name_list = []
                    # fetching the result from the cursor
                    for theme_name in cursor: theme_name_list.append(theme_name)                                      
                  
                    # Query for getting the comparison required details
                    selectionQuery = "SELECT `score_text` FROM `scores_text` WHERE `language_id`=%s AND `scores`=%s"
                    cursor.execute(selectionQuery, (int(language_id), int(result_list[0][0])))
                    
                    # Dictionary for storing result data
                    comparison_list = []
                    # fetching the result from the cursor
                    for comparison_result in cursor: comparison_list.append(comparison_result)                    

                    user_1_style_code = user_1_value
                    user_1_style_id = user_1_style_code[-2:]
                    user_1_theme_id = user_1_style_code[:-2]

                    print("pos",pos,"theme_name_list",theme_name_list[0][0],"comparison_result",comparison_result[0],"user_1_theme_id :",user_1_theme_id,"user_1_style_id :",user_1_style_id," language_id: ",language_id,"user_1_gender_id :",user_1_gender_id)

                    # Query for getting the user1 content required details
                    selectionQuery = "SELECT `content` FROM `free_interpersonal_content` where `theme_id`=%s AND `style_id`=%s AND `language_id`=%s AND `gender_id`=%s AND `self_user`=%s"                                
                    cursor.execute(selectionQuery, (user_1_theme_id, user_1_style_id, language_id, user_1_gender_id, 1))
                    
                    # Dictionary for storing result data
                    user1_content_list = []
                    # fetching the result from the cursor
                    for user1_content_result in cursor: user1_content_list.append(user1_content_result)

                    user_2_style_code = user_2_value
                    user_2_style_id = user_2_style_code[-2:]
                    user_2_theme_id = user_2_style_code[:-2]

                    # Query for getting the user2 content required details
                    selectionQuery = "SELECT `content` FROM `free_interpersonal_content` where `theme_id`=%s AND `style_id`=%s AND `language_id`=%s AND `gender_id`=%s AND `self_user`=%s"                                
                    cursor.execute(selectionQuery, (user_2_theme_id, user_2_style_id, language_id, user_2_gender_id, 0))
                    
                    print("user_2_theme_id :",user_2_theme_id,"user_2_style_id :",user_2_style_id," language_id: ",language_id,"user_2_gender_id :",user_2_gender_id)

                    # Dictionary for storing result data
                    user2_content_list = []
                    # fetching the result from the cursor
                    for user2_content_result in cursor: user2_content_list.append(user2_content_result)                    

                    # forming the list of
                    if(result_list != []): 
                        style_difference_scores.append(result_list[0][0])
                    else:
                        style_difference_scores.append("BLANK")
                            
                    user_1_list.append(user_1_value)
                    user_2_list.append(user_2_value)

                    if(user1_content_list != []):
                        user_1_content.append(user1_content_list[0][0])
                    else:
                        user_1_content.append("BLANK")
                    
                    if(user2_content_list != []):
                        user_2_content.append(user2_content_list[0][0])
                    else:
                        user_2_content.append("BLANK")    
                    
                    if(comparison_result != []):    
                        comparison.append(comparison_result[0])
                    else:
                        comparison.append("BLANK")    
                    
                    if(theme_name_list != []):                           
                        theme.append(theme_name_list[0][0])
                    else:
                        theme.append("BLANK")    
            except:
                # returning the error when there is some error in above try block
                logger.error(traceback.format_exc()) 
                return log_err(config[message_by_language]['QUERY_EXECUTION_STATUS'], 500)
        except:
            # returning the error when there is some error in above try block
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)
        finally:
            cursor.close()
            cnx.close()
        
        # returning success json
        return { 
                    'statusCode': 200,
                    'headers' : {
                                    'Access-Control-Allow-Origin': '*',
                                    'Access-Control-Allow-Credentials': 'true'
                                },
                    'body': json.dumps({"user_1" : user_1_list, "user_2" : user_2_list, "style_difference_scores":style_difference_scores, "theme_order":ordering_list, "user_1_gender":user_1_gender, "user_2_gender":user_2_gender, 'similarity_score':similarity_score, 'user_1_content':user_1_content, 'user_2_content':user_2_content, 'comparison':comparison, 'theme':theme })
                }
    except:
        # returning the error when there is some error in above try block
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
    
if __name__== "__main__":
    handler(None, None)
    