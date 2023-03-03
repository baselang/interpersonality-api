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
13. find_section_theme(): getting section from theme
14. get_guide_report(): generating guide report
15. get_style_pairing_importance(): getting style pairing importance
16. get_section_and_theme_wise_style_pair_summaries(): getting section and theme wise style pair
17. handler(): Handling the incoming request with following steps
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
config.read('user_guides_report.properties')

# message_by_language = os.environ.get('MESSAGES_LANGUAGE')

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

#Setting a parameters of input files
DATA_FILE_THEMES_IN_SECTIONS = os.environ.get('DATA_FILE_THEMES_IN_SECTIONS')
DATA_FILE_STYLE_PAIRING_IMPORTANCE = os.environ.get('DATA_FILE_STYLE_PAIRING_IMPORTANCE')
DATA_FILE_STYLE_PAIR_SUMMARIES = os.environ.get('DATA_FILE_STYLE_PAIR_SUMMARIES')
DATA_FILE_INDIVIDUAL_STYLE_SUMMARIES = os.environ.get('DATA_FILE_INDIVIDUAL_STYLE_SUMMARIES')

#Settting a parameters of Section 0 condition
MAX_SUMMARIES = int(os.environ.get('MAX_SUMMARIES'))
MIN_POSITIVES = int(os.environ.get('MIN_POSITIVES'))
MAX_NEGATIVES = int(os.environ.get('MAX_NEGATIVES'))


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
    pair_report_df.columns = ["Theme",  "user_1_style", "user_2_style", "user_1_text", "user_2_text"]
    
    # returning the final report dataframe
    return pair_report_df


# Function to find element in the list
def findElement(listName, searchElement):
    for value in listName:
        user_1_style_id = value[-2:]
        user_1_theme_id = value[:-2]
        if(int(user_1_theme_id) == searchElement):
            return value
    return '0'    

def find_section_theme(user_1_list,user_2_list,themes_in_sections_input_data,report_name):
    """Find Section number related to theme"""

    #Create dictionary
    SECTION_AND_THEME_DICT = dict()

    #Reassign theme_num values.
    theme_num = list(range(1, 16))

    #Finding section using theme.
    for theme_id in theme_num:

        #Finding user_1_style, user_2_style based on theme_id
        user_1_style = findElement(user_1_list, int(theme_id))
        user_2_style = findElement(user_2_list, int(theme_id))


        if((user_1_style =='0') & (user_2_style=='0')):
            continue

        user_1_style_id = user_1_style[-2:]
        user_2_style_id = user_2_style[-2:]

        themes_wise_sections_input_data =  themes_in_sections_input_data.loc[( themes_in_sections_input_data['theme_id'] == int(theme_id))]

        themes_in_sections = pd.DataFrame()

        #CASE 1.  USER 1 STYLE ID , USER 2 STYLE ID                    
        themes_in_sections =  themes_wise_sections_input_data.loc[( themes_wise_sections_input_data['style_id_1'] == int(user_1_style_id))
        &  ( themes_wise_sections_input_data['style_id_2'] == int(user_2_style_id))]

        if(report_name == 'CouplesGuide'):
            #CASE 2.  USER 2 STYLE ID , USER 1 STYLE ID                    
            if(themes_in_sections.empty):
                themes_in_sections =  themes_wise_sections_input_data.loc[( themes_wise_sections_input_data['style_id_1'] == int(user_2_style_id))
                &  ( themes_wise_sections_input_data['style_id_2'] == int(user_1_style_id))]

        #CASE 3 USER 1 STYLE ID, BLANK                    
        if(themes_in_sections.empty):
            themes_in_sections =  themes_wise_sections_input_data.loc[( themes_wise_sections_input_data['style_id_1'] == int(user_1_style_id))
        &  ( themes_wise_sections_input_data['style_id_2'] == -1)]

        if(report_name == 'CouplesGuide'):
            #CASE 4 BLANK, USER 1 STYLE ID                    
            if(themes_in_sections.empty):
                themes_in_sections =  themes_wise_sections_input_data.loc[( themes_wise_sections_input_data['style_id_1'] == -1)
            &  ( themes_wise_sections_input_data['style_id_2'] == int(user_1_style_id))]


        #CASE 5 BLANK, USER 2 STYLE ID                    
        if(themes_in_sections.empty):
            themes_in_sections =  themes_wise_sections_input_data.loc[( themes_wise_sections_input_data['style_id_1'] == -1)
        &  ( themes_wise_sections_input_data['style_id_2'] == int(user_2_style_id))]

        if(report_name == 'CouplesGuide'):
            #CASE 6 USER 2 STYLE ID, BLANK                    
            if(themes_in_sections.empty):
                themes_in_sections =  themes_wise_sections_input_data.loc[( themes_wise_sections_input_data['style_id_1'] == int(user_2_style_id))
            &  ( themes_wise_sections_input_data['style_id_2'] == -1)]

        #CASE 7 BLANK, BLANK                    
        if(themes_in_sections.empty):
            themes_in_sections =  themes_wise_sections_input_data.loc[( themes_wise_sections_input_data['style_id_1'] == -1)
        &  ( themes_in_sections_input_data['style_id_2'] == -1)]


        if(themes_in_sections.empty):
            print("themes_in_sections empty")
        else:
            section = themes_in_sections['section_id'].values[0]

        theme = []

        try:        
            theme = SECTION_AND_THEME_DICT[section]
            theme.append(theme_id)
        except:
            theme.append(theme_id)

        SECTION_AND_THEME_DICT.update({section: theme})

    return SECTION_AND_THEME_DICT


def get_section_and_theme_wise_style_pair_summaries(section_number,theme_id,user_1_style_id,user_2_style_id,usage,report_name,style_pair_summaries_input_data):
    """Get section_and_theme_wise_style_pair_summaries"""  
    section_and_theme_wise_style_pair_summaries = pd.DataFrame()

    #CASE 1.  USER 1 STYLE ID , USER 2 STYLE ID                    
    section_and_theme_wise_style_pair_summaries = style_pair_summaries_input_data.loc[(style_pair_summaries_input_data['section_id'] == int(section_number))
    &  (style_pair_summaries_input_data['theme_id'] == int(theme_id))
    &  (style_pair_summaries_input_data['style_id_1'] == int(user_1_style_id))
    &  (style_pair_summaries_input_data['style_id_2'] == int(user_2_style_id))
    &  (style_pair_summaries_input_data['usage'] == int(usage))]

    if(report_name == 'CouplesGuide'):
        #CASE 2.  USER 2 STYLE ID , USER 1 STYLE ID                    
        if(section_and_theme_wise_style_pair_summaries.empty):
            section_and_theme_wise_style_pair_summaries = style_pair_summaries_input_data.loc[(style_pair_summaries_input_data['section_id'] == int(section_number))
            &  (style_pair_summaries_input_data['theme_id'] == int(theme_id))
            &  (style_pair_summaries_input_data['style_id_1'] == int(user_2_style_id))
            &  (style_pair_summaries_input_data['style_id_2'] == int(user_1_style_id))
            &  (style_pair_summaries_input_data['usage'] == int(usage))]

    #CASE 3 USER 1 STYLE ID, BLANK                    
    if(section_and_theme_wise_style_pair_summaries.empty):
        section_and_theme_wise_style_pair_summaries = style_pair_summaries_input_data.loc[(style_pair_summaries_input_data['section_id'] == int(section_number))
    &  (style_pair_summaries_input_data['theme_id'] == int(theme_id))
    &  (style_pair_summaries_input_data['style_id_1'] == int(user_1_style_id))
    &  (style_pair_summaries_input_data['style_id_2'] == -1)                            
    &  (style_pair_summaries_input_data['usage'] == int(usage))]

    if(report_name == 'CouplesGuide'):
        #CASE 4 BLANK, USER 1 STYLE ID                    
        if(section_and_theme_wise_style_pair_summaries.empty):
            section_and_theme_wise_style_pair_summaries = style_pair_summaries_input_data.loc[(style_pair_summaries_input_data['section_id'] == int(section_number))
        &  (style_pair_summaries_input_data['theme_id'] == int(theme_id))
        &  (style_pair_summaries_input_data['style_id_1'] == -1)
        &  (style_pair_summaries_input_data['style_id_2'] == int(user_1_style_id))                            
        &  (style_pair_summaries_input_data['usage'] == int(usage))]


    #CASE 5 BLANK, USER 2 STYLE ID                    
    if(section_and_theme_wise_style_pair_summaries.empty):
        section_and_theme_wise_style_pair_summaries = style_pair_summaries_input_data.loc[(style_pair_summaries_input_data['section_id'] == int(section_number))
    &  (style_pair_summaries_input_data['theme_id'] == int(theme_id))
    &  (style_pair_summaries_input_data['style_id_1'] == -1)
    &  (style_pair_summaries_input_data['style_id_2'] == int(user_2_style_id))                            
    &  (style_pair_summaries_input_data['usage'] == int(usage))]


    if(report_name == 'CouplesGuide'):
        #CASE 6 USER 2 STYLE ID, BLANK                    
        if(section_and_theme_wise_style_pair_summaries.empty):
            section_and_theme_wise_style_pair_summaries = style_pair_summaries_input_data.loc[(style_pair_summaries_input_data['section_id'] == int(section_number))
        &  (style_pair_summaries_input_data['theme_id'] == int(theme_id))
        &  (style_pair_summaries_input_data['style_id_1'] == int(user_2_style_id))
        &  (style_pair_summaries_input_data['style_id_2'] == -1)                            
        &  (style_pair_summaries_input_data['usage'] == int(usage))]

    #CASE 7 BLANK, BLANK                    
    if(section_and_theme_wise_style_pair_summaries.empty):                                
        section_and_theme_wise_style_pair_summaries = style_pair_summaries_input_data.loc[(style_pair_summaries_input_data['section_id'] == int(section_number))
    &  (style_pair_summaries_input_data['theme_id'] == int(theme_id))
    &  (style_pair_summaries_input_data['style_id_1'] == -1)
    &  (style_pair_summaries_input_data['style_id_2'] == -1)
    &  (style_pair_summaries_input_data['usage'] == int(usage))]

    return section_and_theme_wise_style_pair_summaries


def get_style_pairing_importance(theme_id,user_1_style_id,user_2_style_id,report_name,style_pairing_importance_input_data):
    """Get style_pairing_importance"""
    #CASE 1. USER 1 STYLE ID , USER 2 STYLE ID
    style_pairing_importance_df = style_pairing_importance_input_data.loc[(style_pairing_importance_input_data['theme'] == int(theme_id))
    &  (style_pairing_importance_input_data['user_style_1'] == int(user_1_style_id))
    &  (style_pairing_importance_input_data['user_style_2'] == int(user_2_style_id))]

    if(report_name == 'CouplesGuide'):
        #CASE 2. USER 2 STYLE ID , USER 1 STYLE ID
        if(style_pairing_importance_df.empty):
            style_pairing_importance_df = style_pairing_importance_input_data.loc[(style_pairing_importance_input_data['theme'] == int(theme_id))
        &  (style_pairing_importance_input_data['user_style_1'] == int(user_2_style_id))
        &  (style_pairing_importance_input_data['user_style_2'] == int(user_1_style_id))]

    #CASE 3. USER 1 STYLE ID , BLANK
    if(style_pairing_importance_df.empty):
        style_pairing_importance_df = style_pairing_importance_input_data.loc[(style_pairing_importance_input_data['theme'] == int(theme_id))
    &  (style_pairing_importance_input_data['user_style_1'] == int(user_1_style_id))
    &  (style_pairing_importance_input_data['user_style_2'] == -1)]

    if(report_name == 'CouplesGuide'):
        #CASE 4. BLANK, USER 1 STYLE ID
        if(style_pairing_importance_df.empty):
            style_pairing_importance_df = style_pairing_importance_input_data.loc[(style_pairing_importance_input_data['theme'] == int(theme_id))
        &  (style_pairing_importance_input_data['user_style_1'] == -1)
        &  (style_pairing_importance_input_data['user_style_2'] == int(user_1_style_id))]


    #CASE 5. BLANK, USER 2 STYLE ID
    if(style_pairing_importance_df.empty):
        style_pairing_importance_df = style_pairing_importance_input_data.loc[(style_pairing_importance_input_data['theme'] == int(theme_id))
    &  (style_pairing_importance_input_data['user_style_1'] == -1)
    &  (style_pairing_importance_input_data['user_style_2'] == int(user_2_style_id))]

    if(report_name == 'CouplesGuide'):
        #CASE 6. USER 2 STYLE ID, BLANK
        if(style_pairing_importance_df.empty):
            style_pairing_importance_df = style_pairing_importance_input_data.loc[(style_pairing_importance_input_data['theme'] == int(theme_id))
        &  (style_pairing_importance_input_data['user_style_1'] == int(user_2_style_id))
        &  (style_pairing_importance_input_data['user_style_2'] == -1)]

    #CASE 7. BLANK, BLANK
    if(style_pairing_importance_df.empty):
        style_pairing_importance_df = style_pairing_importance_input_data.loc[(style_pairing_importance_input_data['theme'] == int(theme_id))
    &  (style_pairing_importance_input_data['user_style_1'] == -1)
    &  (style_pairing_importance_input_data['user_style_2'] == -1)]

    return style_pairing_importance_df


def get_guide_report(user_1_list, user_2_list, report_name, section_num, language_id):
    """Get guide_report"""
    available_summaries = []
    summaries_to_display = []

    theme_id = ''
    section_id  = ''
    usage  = ''
    positive  = ''
    content  = ''  

    section_dataframe_list = []
    dataframe_list = []

    POSITIVE_COUNT = 0
    NEGATIVE_COUNT = 0

    #Reading input CSV file
    themes_in_sections_input_data = pd.read_csv(report_name+'/'+DATA_FILE_THEMES_IN_SECTIONS)
    style_pairing_importance_input_data = pd.read_csv(report_name+'/'+DATA_FILE_STYLE_PAIRING_IMPORTANCE)
    style_pair_summaries_input_data = pd.read_csv(report_name+'/'+DATA_FILE_STYLE_PAIR_SUMMARIES)
    individual_style_summaries_input_data = pd.read_csv(report_name+'/'+DATA_FILE_INDIVIDUAL_STYLE_SUMMARIES)

    # Rename column user_style_1 and user_style_2
    themes_in_sections_input_data = themes_in_sections_input_data.rename(columns = {'user_style_1':'style_id_1'}).rename(columns = {'user_style_2':'style_id_2'})

    SECTION_AND_THEME_DICT = dict()

    #Find section in theme
    SECTION_AND_THEME_DICT  = find_section_theme(user_1_list,user_2_list,themes_in_sections_input_data,report_name)

    # Create a connection with MySQL database for interpersonality.
    cnx = make_connection(PROFILES_HOST, PROFILES_DB_USER, PROFILES_PASSWD, PROFILES_DB_NAME, PROFILES_DB_PORT)
                            
    # Getting the cursor from the DB connection to execute the queries
    cursor = cnx.cursor()

    #Iterate sections 0 to 4
    for section_number in section_num:
        if(section_number == 0):
            #Iterate User1 style and User2 style
            for i in range(len(user_1_list)):
                user_1_style = ''
                user_2_style = ''
                user_1_style = user_1_list[i]
                user_2_style = user_2_list[i]
                theme_id = user_1_style[:-2]
                
                # Query for getting the theme name
                selectionQuery = "SELECT `name` FROM `theme_names` WHERE `language_id`=%s AND `theme_id`=%s"                
                cursor.execute(selectionQuery, (int(language_id), int(theme_id)))
                
                theme_name_list = []
                # fetching the result from the cursor
                for themeName in cursor:                                 
                    theme_name = ''.join(themeName)

                user_1_style_id = user_1_style[-2:]
                user_2_style_id = user_2_style[-2:]
                usage = 0

                section_and_theme_wise_style_pair_summaries = pd.DataFrame()

                # Get section_and_theme_wise_style_pair_summaries dataframe
                section_and_theme_wise_style_pair_summaries = get_section_and_theme_wise_style_pair_summaries(section_number,theme_id,user_1_style_id,user_2_style_id,usage,report_name,style_pair_summaries_input_data)
                
                #If section_and_theme_wise_style_pair_summaries is empty skip process
                if(section_and_theme_wise_style_pair_summaries.empty):
                    continue
                else:
                    section_id = section_and_theme_wise_style_pair_summaries['section_id']
                    usage = section_and_theme_wise_style_pair_summaries['usage']
                    positive = section_and_theme_wise_style_pair_summaries['positive']
                    content = section_and_theme_wise_style_pair_summaries['content']

                style_pairing_importance_df = pd.DataFrame()

                # Get style_pairing_importance dataframe                   
                style_pairing_importance_df = get_style_pairing_importance(theme_id,user_1_style_id,user_2_style_id,report_name,style_pairing_importance_input_data)
                
                # If sstyle_pairing_importance_df.empty assign default value 0 for sorting
                if(style_pairing_importance_df.empty):
                    importance = 0
                else:
                    importance = style_pairing_importance_df['importance'].values[0]

                section_id = section_id.values[0]
                usage = usage.values[0]
                positive = positive.values[0]
                content = content.values[0]

                dataframe_row_list = []
                dataframe_row_list = [section_id,str(theme_id),importance,theme_name,user_1_style_id,user_2_style_id,user_1_style,user_2_style,usage,positive,content]
                section_dataframe_list.append(dataframe_row_list)
               
            available_summaries = pd.DataFrame(section_dataframe_list)

            # Add column into available_summaries
            available_summaries.columns = ["section","theme_id","importance","theme_name","user_1_style_id","user_2_style_id","user_1_style","user_2_style","usage","positive","content"]

            #Sorting available summaries based on importance
            available_summaries.sort_values(["section","importance"], axis=0, ascending=[True,False], inplace=True)

            #reset index value
            available_summaries = available_summaries.reset_index(drop=True)

            #If report CompatibilityGuide, Display the first max_summaries style pair summaries in available_summaries and skip process for section 0
            if(report_name == "CompatibilityGuide"):
                summaries_to_display = pd.DataFrame(available_summaries.head(MAX_SUMMARIES).values.tolist())
                #Adding column
                summaries_to_display.columns = ["section","theme_id","importance","theme_name","user_1_style_id","user_2_style_id","user_1_style","user_2_style","usage","positive","content"]
                continue

            #Get first two positive summary
            summaries_to_display = pd.DataFrame(available_summaries[available_summaries.positive ==1].head(2).values.tolist())

            #Adding column
            summaries_to_display.columns = ["section","theme_id","importance","theme_name","user_1_style_id","user_2_style_id","user_1_style","user_2_style","usage","positive","content"]

            #Removing first two positive summary
            available_summaries.drop(available_summaries[available_summaries.positive == 1 ].head(2).index.values, inplace=True)

            #Algorithm condition 
            while len(summaries_to_display)<MAX_SUMMARIES and len(available_summaries)>0 :
                available_summaries = available_summaries.reset_index(drop=True)

                #Selecting first value of positive column
                if available_summaries['positive'].iloc[0] == 0:
                    if NEGATIVE_COUNT < MAX_NEGATIVES:
                        row_list = []
                        row_list.append(available_summaries.iloc[0].values.tolist())
                        summary_data = pd.DataFrame(row_list)
                        summary_data.columns = ["section","theme_id","importance","theme_name","user_1_style_id","user_2_style_id","user_1_style","user_2_style","usage","positive","content"]
                        summaries_to_display = summaries_to_display.append(summary_data, ignore_index = True)
                        available_summaries.drop([0], inplace = True)
                        NEGATIVE_COUNT = NEGATIVE_COUNT + 1
                        i = i+1                                                        
                    else:
                        available_summaries.drop([0], inplace = True)
                        i = i+1
                else:
                    row_list = []
                    row_list.append(available_summaries.iloc[0].values.tolist())
                    summary_data = pd.DataFrame(row_list)
                    summary_data.columns = ["section","theme_id","importance","theme_name","user_1_style_id","user_2_style_id","user_1_style","user_2_style","usage","positive","content"]
                    summaries_to_display = summaries_to_display.append(summary_data, ignore_index = True)
                    available_summaries.drop([0], inplace = True) 
                    i = i+1
        else:
            #Find section associated theme 
            theme = SECTION_AND_THEME_DICT[section_number]

            #Iterate theme
            for theme_id in theme:
                theme_name = ''
                preframe_style = ''
                style_comparison_self_user = ''
                style_comparison_other_user = ''
                outro_text_style = ''
                
                # Query for getting the theme name
                selectionQuery = "SELECT `name` FROM `theme_names` WHERE `language_id`=%s AND `theme_id`=%s"                
                cursor.execute(selectionQuery, (int(language_id), int(theme_id)))
        
                theme_name_list = []
                # fetching the result from the cursor
                for themeName in cursor:                                 
                    theme_name = ''.join(themeName)

                #Finding user_1_style, user_2_style based on theme_id
                user_1_style = findElement(user_1_list, int(theme_id))
                user_2_style = findElement(user_2_list, int(theme_id))

                #If user_1_style and user_2_style not available skip process.
                if((user_1_style =='0') & (user_2_style=='0')):
                    continue

                user_1_style_id = user_1_style[-2:]
                user_2_style_id = user_2_style[-2:]

                # Usage 1 for Preframe style
                usage = 1
                
                section_and_theme_wise_style_pair_summaries = pd.DataFrame()

                # Get section_and_theme_wise_style_pair_summaries dataframe
                section_and_theme_wise_style_pair_summaries = get_section_and_theme_wise_style_pair_summaries(section_number,theme_id,user_1_style_id,user_2_style_id,usage,report_name,style_pair_summaries_input_data)
            
                #Preframe style pair summary is NULL skip process
                if(section_and_theme_wise_style_pair_summaries.empty):
                    continue         

                #Check theme, user style , section, language, self user in individual style summaries
                self_user_individual_style_summaries_input_data = individual_style_summaries_input_data.loc[(individual_style_summaries_input_data['section_id'] == int(section_number))
                &  (individual_style_summaries_input_data['theme_id'] == int(theme_id))
                &  (individual_style_summaries_input_data['style_id'] == int(user_1_style_id))
                &  (individual_style_summaries_input_data['language_id'] == int(language_id))
                &  (individual_style_summaries_input_data['self_user'] == int(1))]  

                #check self user individual style summaries is NULL skip process 
                if(self_user_individual_style_summaries_input_data.empty):
                    continue

                #Check theme, user style , section, language, other user in individual style summaries
                other_user_individual_style_summaries_input_data = individual_style_summaries_input_data.loc[(individual_style_summaries_input_data['section_id'] == int(section_number))
                &  (individual_style_summaries_input_data['theme_id'] == int(theme_id))
                &  (individual_style_summaries_input_data['style_id'] == int(user_2_style_id))
                &  (individual_style_summaries_input_data['language_id'] == int(language_id))
                &  (individual_style_summaries_input_data['self_user'] == 0)]

                #check other user individual style summaries is NULL skip process
                if(other_user_individual_style_summaries_input_data.empty):
                    continue

                # Usage 2 for outro style
                usage = 2                    
                section_and_theme_wise_outro_style_pair_summaries = pd.DataFrame()
                section_and_theme_wise_outro_style_pair_summaries = get_section_and_theme_wise_style_pair_summaries(section_number,theme_id,user_1_style_id,user_2_style_id,usage,report_name,style_pair_summaries_input_data)                
                             
                style_pairing_importance_df = pd.DataFrame()                    
                style_pairing_importance_df = get_style_pairing_importance(theme_id,user_1_style_id,user_2_style_id,report_name,style_pairing_importance_input_data)                
                
                if(section_and_theme_wise_outro_style_pair_summaries.empty):
                    print("section_and_theme_wise_outro_style_pair_summaries empty")
                else:
                    outro_text_style = section_and_theme_wise_outro_style_pair_summaries['content'].values[0]

                #If style_pairing_importance_df is empty assign default value 0 for sorting.
                if(style_pairing_importance_df.empty):
                    importance = 0
                else:
                    importance = style_pairing_importance_df['importance'].values[0]
                    

                preframe_style = section_and_theme_wise_style_pair_summaries['content'].values[0]
                style_comparison_self_user = self_user_individual_style_summaries_input_data['content'].values[0]
                style_comparison_other_user = other_user_individual_style_summaries_input_data['content'].values[0]
                
                dataframe_row_list = []
                dataframe_row_list = [section_number,str(theme_id),importance,theme_name, preframe_style,style_comparison_self_user,style_comparison_other_user,outro_text_style,user_1_style_id,user_2_style_id,user_1_style,user_2_style]
                dataframe_list.append(dataframe_row_list)

    # Convert list to dataframe                               
    couples_guide_df = pd.DataFrame(dataframe_list)
    if(couples_guide_df.empty):
        print("couples_guide_df is empty")
    else:
        couples_guide_df.columns = ["section","theme_id","importance","theme_name","preframe_style","style_comparison_self_user","style_comparison_other_user","outro_text_style","user_1_style_id","user_1_style_id","user_1_style","user_2_style"]
        couples_guide_df.sort_values(["section","importance"], axis=0, ascending=[True,False], inplace=True)                     

    guides_report = [{"section" : i[0],"theme_id": i[1], "importance" : i[2], "theme_name" : i[3], "user_1_style_id" : i[4], 
            "user_2_style_id" : i[5], "user_1_style" : i[6], "user_2_style" : i[7], "content" : i[10]} for i in summaries_to_display.values.tolist()]        

    couples_guides = [{"section" : i[0],"theme_id": i[1], "importance" : i[2], "theme_name" : i[3], "preframe_style" : i[4], "style_comparison_self_user" : i[5], "style_comparison_other_user" : i[6], "outro_text_style" : i[7], "user_1_style_id" : i[8], "user_2_style_id" : i[9], "user_1_style" : i[10], "user_2_style" : i[11]} for i in couples_guide_df.values.tolist()]

    #Adding Section 0 and Section 1-4 json for combine output.
    guides_report = guides_report +  couples_guides

    # Return json data
    return guides_report

def handler(event,context):
    """Main function for generating reports for each users"""
    
    try:
        # getting the user_id for which report need to be generated
        body = json.loads(event['body'])
        user_id_1 = body['user_id_1']
        user_id_2 = body['user_id_2']
        user_ids = tuple([user_id_1, user_id_2])
        language_id = int(body['language_id'])
        report_name = body['report_name']

        message_by_language = str(language_id) + "_MESSAGES"
    except:
        # returning the error when there is some error in above try block
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
        
    try:
        # 1) i) Reading the input file having mean, sd and regression function of clusters of clusters
        theme_num = list(range(1, 15))

        # Define section values
        section_num = list(range(0, 5)) 
        
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
        
        # 9) Ordering the theme
        ordering_list = [int(theme_no) for theme_no in theme_order.split(",")]
        
        # converting the pair_report dataframe to list
        pair_report_list = pair_report_df.values.tolist()
        user_1_list = []
        user_2_list = []
        
        #2) Retrieve the difference scores for each user pair
        try:
            # Create a connection with MySQL database for interpersonality.
            cnx = make_connection(PROFILES_HOST, PROFILES_DB_USER, PROFILES_PASSWD, PROFILES_DB_NAME, PROFILES_DB_PORT)
                                    
            # Getting the cursor from the DB connection to execute the queries
            cursor = cnx.cursor()
            
            try:                
                for pos in ordering_list:

                    user_1_value = str(int(pair_report_list[pos-1][0])).zfill(2)+str(int(pair_report_list[pos-1][1])).zfill(2)
                    user_2_value = str(int(pair_report_list[pos-1][0])).zfill(2)+str(int(pair_report_list[pos-1][2])).zfill(2)
                                                
                    user_1_list.append(user_1_value)
                    user_2_list.append(user_2_value)

                # Get Guide Report    
                guides_report = get_guide_report(user_1_list, user_2_list, report_name, section_num, language_id)

                
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
                    'body': json.dumps({"guides_report" : guides_report})
                }
    except:
        # returning the error when there is some error in above try block
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
    
if __name__== "__main__":
    handler(None, None)
    
