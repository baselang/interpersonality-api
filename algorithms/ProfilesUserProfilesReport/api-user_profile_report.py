"""
API for generating User Profile Report

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. get_dataframe_list(): getting list of dataframes from the database
4. retrieve_users_report(): retrieve users data from the database
5. interpolate_users_data(): getting users data and interpolating age and gender
6. predicted_score_across_clusters(): getting predicted scores across the cluster for a user
7. calculate_Z_score(): Function to calculate the z-score
8. percentile_score_across_clusters(): Getting percentile score across clusters for a user
9. assign_user_to_style(): Function for assigning user to each style
10. processing_to_assign_user_to_style(): Pre and post processing the function call assign_user_to_style
11. generate_style_summary(): Function to generate individual user report
12. find_childrens(): Function to find the childrens of all parents
13. find_parent_hierarchy(): Function for finding the parent child hierarchy
14. remove_direct_relation(): Function to remove the direct relation if indirect exist
15. calculate_in_degree(): Function to Calculate in_degree of any DataFrame with parent_hierarchy
16. find_childrens_assign(): Function for finding children and assigning in sorted vector
17. find_matched_unmatched_relation(): Finding matched unmatched relation and parent zero degree
18. filter_n_sort_unordered_relation(): Function  for filtering and arranging unordered relation
19. process_before_sorting_relation(): Function for processing the dataframe before arranging it
20. find_stories_list(): Function for finding the stories list
21. add_left_out_style(): Function for adding left out styles into stories list
22. get_order_story_emotional_impact_df(): Function for ordering the story_emotional_impact_df and returning the output
23. get_user_profile_section_data(): Function for getting user profile section data dataframe
24. reduce_DAG(): Function for sorting the origibal DAG as per user's styles
25. handler(): Handling the incoming request with following steps
- Fetching the data from the database
- processing the data and then generating the user profile report for the users pair
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
from itertools import chain
import glob
import fnmatch

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('user_profile_report.properties')

message_by_language = os.environ.get('MESSAGES_LANGUAGE')

# Setting the number of input variables
INPUT_VAR_NUMBER = int(os.environ.get('INPUT_VAR_NUMBER'))

# Setting the total no. of sections that are present
SECTIONS = int(os.environ.get('SECTIONS'))

# Setting the section order for filter process
SECTION_ORDER = os.environ.get('SECTION_ORDER')

#Setting theme and section which need to excluded from filtering process
THEME_EXCLUDED_FROM_FILTERING = int(os.environ.get('THEME_EXCLUDED_FROM_FILTERING')) 
SECTION_EXCLUDED_FROM_FILTERING = int(os.environ.get('SECTION_EXCLUDED_FROM_FILTERING'))

#Setting illustrations count for condition
ILLUSTRATIONS_COUNT =  int(os.environ.get('ILLUSTRATIONS_COUNT'))

#Setting parameter for summary report.
SUMMARY_REPORT_HIGH_VALUE_CONDITION = int(os.environ.get('SUMMARY_REPORT_HIGH_VALUE_CONDITION'))
SUMMARY_REPORT_MIN_VALUE_CONDITION = int(os.environ.get('SUMMARY_REPORT_MIN_VALUE_CONDITION'))
SUMMARY_REPORT_MAX_VALUE_CONDITION = int(os.environ.get('SUMMARY_REPORT_MAX_VALUE_CONDITION'))

#Get CSV File Name 
DATA_FILE_SUMMARY_TEXTS_PRIORITY_AND_SEQUENCE = os.environ.get('DATA_FILE_SUMMARY_TEXTS_PRIORITY_AND_SEQUENCE')
DATA_FILE_SHARE_MODULES = os.environ.get('DATA_FILE_SHARE_MODULES')

# Setting a parameter to choose either of the input files
# Set 1 : To chosse input file 'cluster_personality_types_output.csv'
# Set 2 : To choose input file 'Personalty_type_percentile_scores.csv'
CLUSTER_PT_OUTPUT_OR_PT_PERCENTILE_SCORES = int(os.environ.get('CLUSTER_PT_OUTPUT_OR_PT_PERCENTILE_SCORES'))

# Defining DB paramaters
DB_USER = os.environ.get('DBUSER')
PASSWD = os.environ.get('DBPASSWORD')
HOST = os.environ.get('ENDPOINT')
DB_NAME = os.environ.get('DATABASE')

# Getting the DB details from the environment variables to connect to DB
endpoint = os.environ.get('INTERPERSONALITY_DB_ENDPOINT')
port     = os.environ.get('INTERPERSONALITY_DB_PORT')
dbuser   = os.environ.get('INTERPERSONALITY_DB_DBUSER')
password = os.environ.get('INTERPERSONALITY_DB_DBPASSWORD')
database = os.environ.get('INTERPERSONALITY_DB_DATABASE')

# Files from which data is to be fetched
MEAN_SD_REG_FUN = os.environ.get('MEAN_SD_REG_FUN')
PERCENTILE_SCORES = os.environ.get('PERCENTILE_SCORES')
data_file_name = os.environ.get('DATA_FILE_NAME')

# tables from which data is to be fetched
INPUT_TABLE_NAME = os.environ.get('INPUT_TABLE_NAME')
USER_TABLE_NAME = os.environ.get('USER_TABLE_NAME')
DAG_FILE = os.environ.get('DAG_FILE')

# Interpolating 'age' in the range of -1 to +1 based on the min and max age as 0 and 100 
y1 = int(os.environ.get('Y1_GENDER'))
y2 = int(os.environ.get('Y2_GENDER'))
x1 = int(os.environ.get('X1_AGE'))
x2 = int(os.environ.get('X2_AGE'))

# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
# Setting the log level to INFO
logger.setLevel(logging.INFO)

print("Cold start complete.")

def make_connection():
    """Function to make the database connection."""
    
    # Create a connection with MySQL database.
    return pymysql.connect(host=HOST, user=DB_USER, passwd=PASSWD,
                           db=DB_NAME)

def make_connection_interpersonality_dbserver():
    """Function to make the database connection."""
    return pymysql.connect(host=endpoint, user=dbuser, passwd=password,
        port=int(port), db=database, autocommit=True)    
        
def log_err(errmsg, status_code):
    """Function to log the error messages."""
    print(errmsg)
    return  {
                "statusCode": status_code,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def get_multiple_inputs(theme_num, csv_name):
    """Function for reading multiple files and returning the result list for data in all files"""
    
    # creating the list of filenames with the current path that needed to be fetched by using theme no.
    filenames = [csv_name + str(i) + ".csv" for i in theme_num]
    result_list = []
    
    # Iterating over all the file 
    for i in range(0, len(filenames)):
        # reading each csv and appending it to the result_list
        result_list.append(pd.read_csv(filenames[i]))
        
    # returning the result_list containing data of all the files
    return result_list
    
def retrieve_users_report(user_id, db_connection):
    """Retrieve the user's input data from the MySQL database"""
    
    # constructing the query for fetching data from the tables
    query = "SELECT * FROM {0} input, {1} users WHERE input.user_id = users.user_id AND users.user_id in (%(user_id)s)".format(INPUT_TABLE_NAME, USER_TABLE_NAME)
    
    # Executing the query for fetching data from database
    users_report_df = pd.read_sql(query, db_connection, params={"user_id":user_id})
    
    # Removing the user_id column from the dataset (or removing duplicate column)
    users_report_df = users_report_df.loc[:,~users_report_df.columns.duplicated()]
    
    # Removing the duplicated rows from the dataframe
    users_report_df = users_report_df.loc[~(users_report_df.user_id.duplicated())]
    
    # resetting the index of the dataframe
    users_report_df = users_report_df.reset_index(drop=True)
    
    # Removing unwanted column from the data frame
    users_report_df.drop(["id","timestamp"], axis = 1, inplace = True)
    
    # returning the user report dataframe
    return users_report_df
    
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
        
        # Extracting users data for input variables
        user_input_data = (users_report_df.iloc[0,1:]).to_list()
        
        # Getting the user's predicted score for each cluster
        results1 = [[n*m for n,m in zip(user_input_data, (mean_sd_coef_df.iloc[k,4:]).to_list())] 
                                                        for k in range(0, cluster_of_cluster_count)]
        
        # converting results to dataframe and assigning to another variable coeff_users_prod
        coeff_users_prod = pd.DataFrame(results1)
        
        # Forming a list which contains the summation of two lists from different dataframes
        user_pred_score_clusters = [n+m for n,m in zip((coeff_users_prod.sum(axis=1)).to_list(), 
                                                             (mean_sd_coef_df['intercept']).to_list())] 
        
        # converting the list to dataframe and assigning to the list named user_clust_predscore_df_list
        user_clust_predscore_df_list[i] = pd.DataFrame([user_pred_score_clusters])
        
    # returning the resulted list containing users predicted scores across the clusters for each theme
    return user_clust_predscore_df_list
    
def calculate_Z_score(fuser_score, fmean, fsd):
    """Function to calculate z score"""
    
    # using the scipy library calculating the z-score/percentile in file
    zscore_pct = stats.norm.cdf(fuser_score, loc=fmean, scale=fsd)
    
    # returning the zscore
    return zscore_pct

def percentile_score_across_clusters(user_clust_predscore_df_list, list_mean_sd_reg_fun ,theme_num):
    """Generating the users percentile/z-score for each cluster of cluster"""
    # Defining a list to store data frames of users percentile scores (across 
    # all clusters) for each theme
    user_clust_zscore_df_list = list(range(0, len(theme_num)))
    
    # Iterate over each themes for PT_percentile_score list
    for i in range(0, len(theme_num)):
        
        # Extracting the mean_sd_coef data for the theme from the list
        mean_sd_coef_df = list_mean_sd_reg_fun[i]
        
        # Extracting the PT_percentile_score data for the theme from the list
        user_clust_predscore_vec = user_clust_predscore_df_list[i]
        
        # Extracting number of clusters in the dataframe 'mean_sd_coef_df'
        cluster_of_cluster_count = len(mean_sd_coef_df['cluster'])
        
        # Itrating over each cluster
        zscore_pct = [calculate_Z_score(user_clust_predscore_vec.iloc[0,j], mean_sd_coef_df['mean'].iloc[j], 
                            mean_sd_coef_df['stdev'].iloc[j]) for j in range(0, cluster_of_cluster_count)]
        
        # inserting the resulted zscore_pct dataframe into user_clust_zscore_df_list
        user_clust_zscore_df_list[i] = pd.DataFrame([zscore_pct])
        
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
        sum_of_dot_product = sum([n*m for n,m in zip(f_user_percentile_score.iloc[0,:],style_score)])
        
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

def processing_to_assign_user_to_style(user_clust_zscore_df_list, list_PT_percentile_score, theme_num):
    """Pre and post processing for function call"""
    
    # Defining a dataframe to store the details about user assignment to style and angle
    user_theme_style_angle_df = pd.DataFrame(np.full((0,4), np.nan))
    
    # Iterate over each theme
    for j in range(0, len(theme_num)):
        # Getting the users zscore across the cluster for the theme
        users_percentile_score_df = user_clust_zscore_df_list[j]
        
        # Getting the user zscore: index by 0 row, so it will fetch only the first row at a time since the is only one user
        # theme df interate using the j for loop, adjusting by 0.5
        # iterating over the len of the users columns
        user_percentile_score = users_percentile_score_df - 0.5
        
        # Getting the data frame of percentile score of theme-style
        styles_percentile_score_df = list_PT_percentile_score[j]
        
        # Adjusting by 'styles_percentile_score_df' 0.5
        styles_percentile_score_df.iloc[:,1:] = styles_percentile_score_df.iloc[:,1:] - 0.5
        
        # Function call to assign user to one style in the theme
        style_angle = assign_user_to_style(styles_percentile_score_df, user_percentile_score)
        
        # Making the row with required details
        details_row = [0+1, j+1, style_angle[0]+1, style_angle[1]]
        
        # assigning data to the user theme style angle list
        user_theme_style_angle_df.loc[j] = details_row
    
    # Assigning column names to the dataframe     
    user_theme_style_angle_df.columns = ["user","theme","style","angle"]
    
    # returning the resulted theme style angle dataframe
    return user_theme_style_angle_df

def generate_style_summary(user_number, user_theme_style_angle_df, styles_report_df):
    """Function to generate individual user report"""
    
    # Filtring rows related to the user
    user_style_assignment_df = user_theme_style_angle_df.loc[user_theme_style_angle_df.user == user_number+1]
    
    # Getting theme, style and angle data from filtred rows
    user_theme_num = user_style_assignment_df['theme']
    user_style_num = user_style_assignment_df['style']
    dihedral_angle = user_style_assignment_df['angle']
    
    # Defining data frame to store style summaries for all style assigned to user
    theme_style_unsorted = pd.DataFrame()
    
    
    for j in range(0,len(user_theme_num)):
        # To filter the rows corresponding to theme from 'styles_report_df', theme_num
        # is used instead of 'user_theme_num'.
        theme_style = styles_report_df.loc[(styles_report_df['Theme']==int(user_theme_num[j]))
                                             & (styles_report_df['Style']==int(user_style_num[j]))]
        
        # Checking whether sections are available or not for the theme_style
        if not theme_style.empty:
            theme_style = theme_style.assign(angle = [dihedral_angle[j] for k in range(0, len(theme_style.axes[0]))])
            
            theme_style_unsorted = theme_style_unsorted.append(theme_style, ignore_index = True)
            
    # Sorting the data, first, as per section and next as per angle
    theme_style_sorted = theme_style_unsorted.sort_values(["Section", "angle"], ascending = (True, True))
    
    return pd.DataFrame(theme_style_sorted)

def find_childrens(f_parent, first_parent_fun, DAG_section_data, user_styles, reduced_DAG):
    """Function to find the childrens of all parents"""
    # Fetching the children of the parent
    childrens = DAG_section_data.loc[DAG_section_data['Parent']==f_parent]['Child']
    
    # iterate over each childern
    for child in childrens:
        # Checking if child is in user's styles or not
        if user_styles.isin([child]).any():
            
            # Checking if parent is in user's style or not
            if user_styles.isin([f_parent]).any():
                
                # If both above condition true then assign parent child relation into new DAG
                reduced_DAG = reduced_DAG.append(pd.Series([f_parent,child], index = reduced_DAG.columns), ignore_index=True)
                
                # and point the first_parent_fun to f_parent
                first_parent_fun = f_parent
                
            else:
                reduced_DAG = reduced_DAG.append(pd.Series([first_parent_fun,child], index = reduced_DAG.columns), ignore_index=True)
            
        # Calling recursive function again for finding next layer of childern
        reduced_DAG = find_childrens(child, first_parent_fun, DAG_section_data, user_styles, reduced_DAG) 
    
    # sending the reduced dag
    return reduced_DAG
    
def find_parent_hierarchy(user_style_sections_data, DAG_section_data, ls, user_styles, reduced_DAG, parent_child):
    """Function for finding the parent child hierarchy"""
    for i in range(0, len(DAG_section_data.axes[0])):
        # Getting the parent and child number from DAG_section_data
        parent_child = DAG_section_data.loc[i]
        
        parent = parent_child[0]
        
        # Function call to find the childrens of all parents
        parent_hierarchy = find_childrens(parent, parent, DAG_section_data, user_styles, reduced_DAG)
        
        reduced_DAG = parent_hierarchy
        
    # Remove duplicated relation
    parent_hierarchy = parent_hierarchy.drop_duplicates()
    
    # Keep only the relation whose parents are in users
    parent_hierarchy = parent_hierarchy.loc[parent_hierarchy[0].isin(user_styles)]
        
    # Assign colnames to DF
    parent_hierarchy.columns = ['parent', 'child']
    
    return parent_hierarchy
    
def remove_direct_relation(parent_hierarchy):
    """Function to remove the direct relation if indirect exist"""
    # Table of childrens frequency
    child_freq_table = pd.DataFrame(parent_hierarchy.groupby('child').child.count() >= 2)
    
    # Find the repeated child in dataframe
    repeated_child = (child_freq_table.loc[child_freq_table['child']==True]).index.to_list()
    
    if len(repeated_child) >= 1:
        # Iterate over each repeated child
        
        for k in repeated_child:
            # iterate over each parent of repeated child
            parents_rep_child = parent_hierarchy[parent_hierarchy['child']==int(k)]['parent']
            
            # iterate over each parent of repeated child
            for pp in parents_rep_child:
                
                flag2 = parent_hierarchy['child'].isin([pp])
                
                # If child have more than 1 parent
                if flag2.sum()>=1:
                    parent_remove = parent_hierarchy.loc[flag2]['parent']
                    
                    parent_hierarchy = parent_hierarchy.loc[~(parent_hierarchy['parent'].isin(parent_remove.to_list()) & parent_hierarchy['child'].isin([k]))]
                    
    return parent_hierarchy

def calculate_in_degree(parents_seq, parent_hierarchy):
    """Function to Calculate in_degree of any DataFrame with parent_hierarchy"""
    # fetching the length of parent_seq column
    parents_count = len(parents_seq)
    
    # Define vector to store the in-degree of parents
    in_degree = [0]*parents_count
    
    for pc in range(0,parents_count):
        
        # Calculating the in degree of supplied parents
        in_degree[pc] = (parent_hierarchy['child'].isin([parents_seq[pc]])).sum()
        
    return in_degree
    
def find_childrens_assign(f_parent, parent_hierarchy, parent_hierarchy_index, sorted_vector):
    """Function for finding children and assigning in sorted vector"""
    
    # Find all the childers of the parent
    childrens = parent_hierarchy.loc[parent_hierarchy['parent'].isin([f_parent])]['child']
    
    
    # Remove the relation of the parent with his childrens
    parent_hierarchy = parent_hierarchy[~(parent_hierarchy['parent'].isin([f_parent]))]
    
    child_in_degree = calculate_in_degree(childrens.to_list(), parent_hierarchy)
    
    # Get the childrens with in-degree zero
    f_zero_degree_childrens = childrens[[True if i==0 else False for i in child_in_degree]]
    
    if len(f_zero_degree_childrens) > 1 :
        f_zero_degree_childrens_index = [0]*len(f_zero_degree_childrens)
        
        for xx in range(0, len(f_zero_degree_childrens)):
            
            f_zero_degree_childrens_index[xx] = (parent_hierarchy_index.loc[parent_hierarchy_index['child'].isin([f_zero_degree_childrens.iloc[xx]])]['index']).min()
        
        # finding order of zero_degree_parent_index
        temp2 = sorted(f_zero_degree_childrens_index)
        res = [f_zero_degree_childrens_index.index(i) for i in temp2]
        
        f_zero_degree_childrens = f_zero_degree_childrens.reset_index(drop=True)
        
        # ordering parents_zero_degree on the basis of order in res i.e. order in zero_degree_parent_index
        f_zero_degree_childrens = pd.Series([f_zero_degree_childrens[i] for i in res])
    
    if f_zero_degree_childrens.size > 0:
        if np.isnan(f_zero_degree_childrens.iloc[0]):
            f_zero_degree_childrens.iloc[0] = 0
    
        for child_in in f_zero_degree_childrens:
            # Remove the relation of the parent with his childrens
            
            parent_hierarchy_index = parent_hierarchy_index.loc[~((parent_hierarchy_index['parent'].isin([f_parent])) & (parent_hierarchy_index['child'].isin([child_in])))]
            
            # Appending child into sorted vector   
            sorted_vector.append(child_in)
            
            # Recursive call to find childern of the child
            final_vector, parent_hierarchy, parent_hierarchy_index, sorted_vector = find_childrens_assign(child_in, parent_hierarchy, parent_hierarchy_index, sorted_vector)
    
    return sorted_vector, parent_hierarchy, parent_hierarchy_index, sorted_vector
    
def find_matched_unmatched_relation(parent_hierarchy, DAG_section_data, in_degree):
    """Finding matched unmatched relation and parent zero degree"""
    # Filter the unique parents having 0 degree
    parents_zero_degree = pd.Series((parent_hierarchy['parent'].loc[[i==0 for i in in_degree]]).unique())
    
    # Index the relations in DAG if it found in DAG
    relation_index = [0]*len(parent_hierarchy.axes[0])
    
    # 3) i) Ordering of the parent child relation in reduced DAG
    for m in range(0, len(parent_hierarchy.axes[0])):
        # Get the relation from reduced DAG
        relation = parent_hierarchy.iloc[m]
        
        #Iterate over each relation in original DAG
        for nr in range(0,len(DAG_section_data.axes[0])):
            # Get the relation from raw DAG
            mix_relation = DAG_section_data.iloc[nr]
            
            # matching relation with mix relation
            condition1 = relation['parent'] == mix_relation['Parent'] and relation['child']==mix_relation['Child']
            
            if condition1:
                relation_index[m] = nr+1
            
    # Filtering the rows having found relation in DAG and storing in DF
    matched_relation_df = parent_hierarchy.loc[[i!=0 for i in relation_index]]
    
    # Fetching the index of matched relations (relation found in  DAG)
    matched_relation_df = matched_relation_df.assign(index = [i-1 for i in relation_index if i!=0])
    
    # Filter the unmatched/unordered relation
    unordered_relations_df = parent_hierarchy.loc[[i==0 for i in relation_index]]
    
    unordered_relations_df = unordered_relations_df.reset_index(drop=True)
    
    # returning the response
    return unordered_relations_df, parent_hierarchy, matched_relation_df, parents_zero_degree, in_degree
    
def filter_n_sort_unordered_relation(DAG_section_data, unordered_relations_df):
    """Function  for filtering and arranging unordered relation"""
    # Fetch the parent from the relation
    unordered_parent = unordered_relations_df['parent']
                
    # Define list to store the index of unmatched relation
    index1 = [0]*len(unordered_parent)
    
    # Iterate over each parent in unmatched relation
    for up in range(0, len(unordered_parent)):
        
        index_unordered = (DAG_section_data.loc[DAG_section_data['Parent']==unordered_parent[up]].index.to_list())[0]
        
        index1[up] = index_unordered + 1
    
    # Check whether thre is repeatation of parent in unmatched parent 
    if ((pd.DataFrame(index1).groupby(0)[0].count()>1).sum())>=1:
        
        tem_unordered_parent_tb = pd.DataFrame(pd.DataFrame(unordered_parent).groupby('parent')['parent'].count()>1)
        
        # find the childrens or unordered identical parents
        identical_parents = (tem_unordered_parent_tb.loc[tem_unordered_parent_tb['parent']==True]).index.to_list()
        
        # find the childrens or unordered identical parents
        identical_par_child = unordered_relations_df.loc[unordered_relations_df['parent'].isin(identical_parents)]['child']
        
        # Define index vector to store order of duplicated parents in unmatched parents
        index2 = [0]*len(identical_par_child)
        
        # Itreate over each child of the duplicated parent and index the parent as 
        # par child occurance in DAG
        for ipc in range(0,len(identical_par_child)):
            
            index2[ipc] = (DAG_section_data.loc[DAG_section_data['Child']==identical_par_child.iloc[ipc]].index.to_list())[0]
            
        ind2 = 0
                    
        for ind, rl in enumerate(unordered_relations_df['parent'].isin(identical_parents)):
            if rl==True:
                index1[ind] = index2[ind2] + 1
                ind2 +=1
    
    # Assigning Adding index column in unordered/unmatched parents df
    # Raising its index by 1000 so it gets into sorting after matched relation
    unordered_relations_df['index'] = pd.DataFrame(index1) + 1000
    
    # returning the unordered relation
    return unordered_relations_df, DAG_section_data
    
def process_before_sorting_relation(parent_hierarchy_index, parents_zero_degree):
    """Function for processing the dataframe before arranging it"""
    # Defining a list to store the stories 
    stories_list = []
    
    # Defining a vector to store the index of zero degree parents
    zero_degree_parent_index = [0]*len(parents_zero_degree)
    
    # resetting the index of parents_zero_degree
    parents_zero_degree = parents_zero_degree.reset_index(drop=True)
    
    for yy in range(0,len(parents_zero_degree)):
        
        zero_degree_parent_index[yy] = (parent_hierarchy_index.loc[parent_hierarchy_index['parent'].isin([parents_zero_degree[yy]])]['index']).min()
    
    # finding order of zero_degree_parent_index
    temp = sorted(zero_degree_parent_index)
    res = [zero_degree_parent_index.index(i) for i in temp]
    
    
    # ordering parents_zero_degree on the basis of order in res i.e. order in zero_degree_parent_index
    parents_zero_degree = pd.DataFrame([parents_zero_degree[i] for i in res])
    
    # Defining a list to store the stories 
    stories_list = [0]*len(parents_zero_degree)
    
    # returning the preprocessed data
    return stories_list, parents_zero_degree, zero_degree_parent_index
    
def find_stories_list(parent_hierarchy_index, parents_zero_degree, stories_list, parent_hierarchy):
    """Function for finding the stories list"""
    # iterate over each unique sorted parent
    for p in range(0, len(parents_zero_degree)):
        sorted_vector = []
        
        # storing the parent in sorted_vector
        sorted_vector.append(parents_zero_degree.iloc[p])
        
        # find the imidiate childeren of the parent
        children_level = parent_hierarchy.loc[parent_hierarchy['parent'].isin([parents_zero_degree.iloc[p]])]['child']
        
        # resetting the index of children level dataframe 
        children_level = children_level.reset_index(drop=True)
        
        # Remove the relation of the parent with his childrens
        parent_hierarchy = parent_hierarchy.loc[~(parent_hierarchy['parent'].isin([parents_zero_degree.iloc[p]]))]
        
        # Find the indegree of the childrens
        in_degree_level1 = calculate_in_degree(children_level.to_list(), parent_hierarchy)
        
        # Get the childrens with in-degree zero
        zero_degree_childrens = children_level[[True if i==0 else False for i in in_degree_level1]]
        
        # If there is zero degree childrens are more than one than decide their order
        if len(zero_degree_childrens) > 1:
            
            zero_degree_childrens_index = [0]*len(zero_degree_childrens)
            
            for xx in range(0, len(zero_degree_childrens)):                
                zero_degree_childrens_index[xx] = (parent_hierarchy_index[parent_hierarchy_index['child'].isin([zero_degree_childrens[xx]])]['index']).min()
                
            # finding order of zero_degree_parent_index
            temp1 = sorted(zero_degree_childrens_index)
            res = [zero_degree_childrens_index.index(i) for i in temp1]
            
            # ordering parents_zero_degree on the basis of order in res i.e. order in zero_degree_parent_index
            zero_degree_childrens = pd.DataFrame([zero_degree_childrens[i] for i in res])
            
        # Check the count of zero degree childrens and call function to find their 
        # children if any
        if len(zero_degree_childrens) > 0 :
            for k in range(0,len(zero_degree_childrens)):
                
                # Remove the relation of the parent with his childrens
                parent_hierarchy_index = parent_hierarchy_index.loc[~(parent_hierarchy_index['parent'].isin([parents_zero_degree.iloc[p]]) & parent_hierarchy_index['child'].isin([zero_degree_childrens.iloc[k]]))]
                
                # Add zero_degree_childrens in sorted_vector
                sorted_vector.append(int(zero_degree_childrens.iloc[k]))
                
                final_vector, parent_hierarchy, parent_hierarchy_index, sorted_vector = find_childrens_assign(zero_degree_childrens.iloc[k], parent_hierarchy, parent_hierarchy_index, sorted_vector)
                            
            stories_list[p] = final_vector
        else :
            stories_list[p] = sorted_vector
            
    return stories_list
    
def add_left_out_style(stories_list, user_styles):
    """Function for adding left out styles into stories list"""
    stories_list = [[int(j.to_list()[0]) if isinstance(j, pd.Series) else j for j in i] for i in stories_list]
    
    # Making the vector of the list 'stories_list'
    stories_list_vec =  [int(i) for i in list(chain.from_iterable(stories_list))]
    
    # Getting the left out styles from user list 
    left_out_style = user_styles.loc[~(user_styles.isin(stories_list_vec))]
    
    if len(left_out_style)>0:
        stories_list.extend([[i] for i in left_out_style.to_list()])
    
    # returning the stories list    
    return stories_list
    
def get_order_story_emotional_impact_df(stories_list, user_section_wise_data):
    """Function for ordering the story_emotional_impact_df and returning the output"""
    # Define a data frame to store story number and best angle


    story_emotional_impact_df = pd.DataFrame(np.full((len(stories_list), 2), np.nan))
    
    # Find the best dihedral angle of the stories
    first_style = [0]*len(stories_list)
    
    # Iterate over each story in list
    for sl in range(0, len(stories_list)):
        # Getting the styles number from the story
        styles_vec = stories_list[sl]
        
        first_style[sl] = styles_vec[0]
         
        # Fetch the themes of the styles
        theme_numbers = user_section_wise_data.loc[user_section_wise_data['Number'].isin(styles_vec)]['Theme']

        # Get the best dihedral angle for the story
        emotional_impact = user_section_wise_data.loc[user_section_wise_data['Number'].isin(styles_vec)]['emotional_impact']
        
        # Find the best angle among the styles' angle 
        best_emotional_impact = emotional_impact.max()

        
        story_emotional_impact_df.loc[sl] = [sl, best_emotional_impact]
            
    first_style = [int(j.to_list()[0]) if isinstance(j, pd.Series) else j for j in first_style]
    
    story_emotional_impact_df = story_emotional_impact_df.assign(first_style = first_style)
    
    story_emotional_impact_df.columns = ['story','emotional_impact', 'first_style']
    
    ordered_story_emotional_impact_df = story_emotional_impact_df
    
    ordered_story_emotional_impact_df.sort_values("emotional_impact", axis = 0, ascending = False, inplace = True)
    
    # resetting the index of the dataframe
    ordered_story_emotional_impact_df = ordered_story_emotional_impact_df.reset_index(drop=True)
    
    if len(ordered_story_emotional_impact_df.axes[0]) > 4:
        
        # Fetching the rows of df except first two and last rows
        remaining_df = ordered_story_emotional_impact_df.loc[2:len(ordered_story_emotional_impact_df.axes[0])-2]
        
        # Fetching the last row of the df
        last_row_df = pd.DataFrame([ordered_story_emotional_impact_df.loc[len(ordered_story_emotional_impact_df.axes[0])-1]])
        
        # Fetching the first two rows of the df
        ordered_story_emotional_impact_df = ordered_story_emotional_impact_df.loc[0:1]
        
        # Arranging the remaining_df based on their first styles
        remaining_df = remaining_df.sort_values("first_style", axis = 0, ascending = True)
        
        # Binding all dataframes
        ordered_story_emotional_impact_df = pd.concat([ordered_story_emotional_impact_df, remaining_df, last_row_df])
        
        # resetting the index of the dataframe
        ordered_story_emotional_impact_df = ordered_story_emotional_impact_df.reset_index(drop=True)     
            
    return ordered_story_emotional_impact_df
    
def get_user_profile_section_data(styles_report_df, ordered_story_emotional_impact_df, stories_list, ls):
    """Function for getting user profile section data dataframe"""
    
    user_profile_df_section = pd.DataFrame()
    
    section_styles_report = styles_report_df.loc[styles_report_df['Section']==ls]
    
    for sl in range(0,len(ordered_story_emotional_impact_df.axes[0])):

        # Fetching story number
        story_number = ordered_story_emotional_impact_df.loc[sl]['story']
        
        # Extracting styles 
        styles_of_story = stories_list[int(story_number)]
        
        styles_df = pd.DataFrame()
        
        # Iterating over each style in story sl
        for xx in range(0,len(styles_of_story)):
            
            section_story_style_df = section_styles_report.loc[section_styles_report['Number'].isin([styles_of_story[xx]])]            

            rows_count = len(section_story_style_df.axes[0])
            
            if rows_count > 0:
                #Fetching the styles data from 'styles_report_df'
                if len(styles_df.axes[0]) != 0:
                    styles_df = pd.concat([styles_df, section_story_style_df])
                else:
                    styles_df = section_story_style_df
                        
        
        if len(styles_df.axes[0]) > 0:
            
            xll = [sl+1]*len(styles_df.axes[0])
            
            # Adding story number in the DF its temprory output
            styles_df = styles_df.assign(story = xll)
            
            if len(user_profile_df_section.axes[0]) != 0:
                user_profile_df_section = pd.concat([user_profile_df_section, styles_df])
            else:
               user_profile_df_section = styles_df
               
    return user_profile_df_section
        
    
def reduce_DAG(list_user_reports, DAG_section_data_list, styles_report_df, user_id):
    """Function for sorting the origibal DAG as per user's styles"""
    
    # Defining data frame to store summary of users profile
    user_profile_report_stats_df = pd.DataFrame(np.full((1,19), np.nan))
    
    user_profile_df = pd.DataFrame()
    
    # Fetch users data
    user_style_sections_data = list_user_reports
    
    # Iterating over each sections
    for ls in range(1,SECTIONS+1):
        
        # Define the data frame to store reduced DAG for the user
        reduced_DAG = pd.DataFrame(np.full((0,2), np.nan))
        
        # Fetch user's section wise data
        user_section_wise_data = user_style_sections_data.loc[user_style_sections_data['Section'] == ls]
        
        # Fetch user's styles for the section
        user_styles = user_style_sections_data.loc[user_style_sections_data['Section']==ls]['Number']
        
        # Define vector to store parent child relation
        parent_child = [0]*2
        
        # Fetching the DAG data for ls section
        DAG_section_data = DAG_section_data_list[ls-1]
        
        # finding the parent child hierarchy
        parent_hierarchy = find_parent_hierarchy(user_style_sections_data, DAG_section_data, ls, user_styles, reduced_DAG, parent_child)
        
        # 2)i) Remove the direct relation if indirect exist
        parent_hierarchy = remove_direct_relation(parent_hierarchy)
        
        # resetting the index of the dataframe
        parent_hierarchy = parent_hierarchy.reset_index(drop=True)
        
        
        # 3) Sorting the order of the parents according to their occurance in DAG
        if len(parent_hierarchy.axes[0])>0:
            
            # Calculate in_degree of parents 
            in_degree = calculate_in_degree(parent_hierarchy['parent'], parent_hierarchy)
            
            # finding matched unmatched relation and parent zero degree
            unordered_relations_df, parent_hierarchy, matched_relation_df, parents_zero_degree, in_degree = find_matched_unmatched_relation(parent_hierarchy, DAG_section_data, in_degree)
            
            # If there is unmatched relation exist
            if len(unordered_relations_df.axes[0])>0:
                # function for filtering and arranging unordered relation df
                unordered_relations_df, DAG_section_data = filter_n_sort_unordered_relation(DAG_section_data, unordered_relations_df)
            
            # Binding the matched and unmatched relations dataframe 
            parent_hierarchy_index = pd.concat([matched_relation_df, unordered_relations_df])
            
            parent_hierarchy_index = parent_hierarchy_index.reset_index(drop=True)
            
            # 4) Sorting the relations
            
            stories_list, parents_zero_degree, zero_degree_parent_index = process_before_sorting_relation(parent_hierarchy_index, parents_zero_degree)
                
            # finding the stories list
            stories_list = find_stories_list(parent_hierarchy_index, parents_zero_degree, stories_list, parent_hierarchy)
        else:
            # if there is now row inside the parent_hierarchy
            stories_list = [0]*len(user_styles.axes[0])
            
            # putting user styles into the stories list
            for yy in range(0, len(user_styles)):
                stories_list[yy] = [user_styles.iloc[yy]]
                
        
        # 5) Adding the left out styles in stories_list
        
        stories_list = add_left_out_style(stories_list, user_styles)
        
            
        # 6) Sorting of stories for generating user profile
        
        ordered_story_emotional_impact_df = get_order_story_emotional_impact_df(stories_list, user_section_wise_data)

        # calling function for getting the section related user_profile_df
        user_profile_df_section = get_user_profile_section_data(styles_report_df, ordered_story_emotional_impact_df, stories_list, ls)
        
        
        if len(user_profile_df_section.axes[0]) > 0:
            
            if len(user_profile_df.axes[0]) != 0:
                # merging single Section output into  the user_profile_df
                user_profile_df = pd.concat([user_profile_df, user_profile_df_section])
                
            else :
                # when user_profile_df has no dataframe than assigning section dataframe to user_profile_df
                user_profile_df = user_profile_df_section
    
    # returning the user_profile_report for individual user
    return user_profile_df


def filtering_current_report_content(list_user_reports,extended_report_content_df,theme_num,section_num):
    """Function to filter current report content"""
    extended_report_data = pd.DataFrame()

    SECTION_TARGET_DICT = {"SECTION_TARGET_1": 600, "SECTION_TARGET_2": 900,"SECTION_TARGET_3": 600, "SECTION_TARGET_4": 500, "SECTION_TARGET_5": 500}

    oversize_sections = pd.DataFrame()
    filtering_candidates = pd.DataFrame()
    temp = pd.DataFrame()

    # Finding oversize sections
    for section_number in section_num:
        section_wise_list = list_user_reports.loc[(list_user_reports['Section']==int(section_number))]
        SECTION_TARGET_WORD_COUNT = SECTION_TARGET_DICT["SECTION_TARGET_"+str(section_number)]        
        if(section_wise_list['word_count'].sum() >= SECTION_TARGET_WORD_COUNT):            
            # converting the list to dataframe and assigning to the dataframe named section_wise_list_df
            section_wise_list_df = pd.DataFrame(section_wise_list)            
            oversize_sections = oversize_sections.append(section_wise_list_df)

    #checking oversize_sections is empty
    if(oversize_sections.empty):
        return extended_report_content_df,list_user_reports 

    filtering_candidates = filtering_candidates.append(oversize_sections)


    #Removing Theme 11 styles in Section 1 from filtering_candidates    
    filtering_candidates.drop(filtering_candidates.loc[(filtering_candidates['Theme']==THEME_EXCLUDED_FROM_FILTERING) & (filtering_candidates['Section']==SECTION_EXCLUDED_FROM_FILTERING)].index, inplace=True)

    temp = temp.append(filtering_candidates)

    #Checking to see if the section that style summary appears in has <= 2 illustrations.
    #If above comment is true and the style summary does have an illustration, remove that style summary from filtering_candidates
    for section_number in section_num:
        section_wise_filtering_candidates = filtering_candidates.loc[(filtering_candidates['Section']==int(section_number))]
        if(section_wise_filtering_candidates['Illustration'].sum() <= ILLUSTRATIONS_COUNT):
            filtering_candidates.drop(filtering_candidates.loc[(filtering_candidates['Illustration']==int(1)) & (filtering_candidates['Section']==int(section_number))].index, inplace=True)

    if(filtering_candidates.empty):
        return extended_report_content_df,list_user_reports
    else:
        lowest_emotional_impact = filtering_candidates['emotional_impact'].min()        

    filtering_candidates.drop(filtering_candidates.loc[(filtering_candidates['emotional_impact']>int(lowest_emotional_impact))].index, inplace=True)


    max_repetitions = 0

    #Count the number of repetitions of each Theme in filtering_candidates and find max_repetitions.
    for theme_number in theme_num:
        temp = filtering_candidates.loc[filtering_candidates['Theme']==int(theme_number)]
        if(max_repetitions <= temp['Theme'].count()):
            max_repetitions = temp['Theme'].count()

    #Remove style summaries whose themes that have < max_repetitions from filtering_candidates
    for theme_number in theme_num:
        temp = filtering_candidates.loc[filtering_candidates['Theme']==int(theme_number)]
        if(max_repetitions > temp['Theme'].count()):
           filtering_candidates.drop(filtering_candidates.loc[(filtering_candidates['Theme']==int(theme_number))].index, inplace=True)

    ordering_list = [int(section_no) for section_no in SECTION_ORDER.split(",")]

    #Scan filtering_candidates looking for styles summaries that appear in sections in the section order
    for section_value in ordering_list:
        section_wise_filtering_candidates = filtering_candidates.loc[filtering_candidates['Section']==int(section_value)]   
        if(section_wise_filtering_candidates.empty):
            print("-Empty-")
        else:
            break    

    #Add extended content data       
    extended_report_content_df = extended_report_content_df.append(section_wise_filtering_candidates.iloc[:1])

    #Remove extended content data
    list_user_reports.drop(section_wise_filtering_candidates.iloc[:1].index, inplace=True)

    #Recursive function call
    extended_report_content_df,list_user_reports = filtering_current_report_content(list_user_reports,extended_report_content_df,theme_num,section_num)

    return extended_report_content_df,list_user_reports 


def get_extended_report_content(list_user_reports,section_num):
    """Function to get extended report content"""

    extended_report_content_df = pd.DataFrame()

    highest_count_style_summaries  = 0
    section_no_highest_count_style_summaries  = 0

    # Finding highest style summaries cound and section number in which it belongs.
    for section_number in section_num:
        temp = list_user_reports.loc[list_user_reports['Section']==int(section_number)]
        if(highest_count_style_summaries < temp['Number'].count()):
           highest_count_style_summaries = temp['Number'].count()
           section_no_highest_count_style_summaries = section_number

    temp = list_user_reports.loc[list_user_reports['Section']==int(section_no_highest_count_style_summaries)]

    # Selecting lowest emotional impact ratio
    lowest_emotional_impact = temp['emotional_impact'].min()        

    # Sorting the data, based on emotional_impact
    temp = temp.sort_values(["emotional_impact"], ascending = (True))

    extended_report_content_df = temp.iloc[:1]

    #deleting from current report.
    list_user_reports.drop(temp.iloc[:1].index, inplace=True)


    return extended_report_content_df, list_user_reports



def get_summaries_report_content(list_user_reports):
    """Function to get summaries report content"""
    available_summaries = pd.DataFrame()
    summary_data_user = pd.read_csv(DATA_FILE_SUMMARY_TEXTS_PRIORITY_AND_SEQUENCE)
    summary_data_user.sort_values(["value", "Sequence"], axis=0, ascending=[False,True], inplace=True) 
    available_summaries = summary_data_user[summary_data_user.Style.isin(list_user_reports.Number)] 
    available_summaries_filtered = available_summaries[available_summaries.value >= SUMMARY_REPORT_HIGH_VALUE_CONDITION].count()
    count_high_value = available_summaries_filtered['value']      
    number_to_display = min(count_high_value, SUMMARY_REPORT_MIN_VALUE_CONDITION)
    number_to_display=max(number_to_display, SUMMARY_REPORT_MAX_VALUE_CONDITION)      
    available_summaries = available_summaries.head(number_to_display).sort_values(["Sequence"])
    return available_summaries


def handler(event,context):
    """Main function for generating reports for each users"""
    try:
        # getting the user_id for which report need to be generated
        body = json.loads(event['body'])
        user_id = body['user_id']
        section_id = int(body['section_id'])
        self_user = int(body['self_user'])
        language_id = int(body['language_id'])
        #report_tab  value  #INDIVIDUAL_REPORT #INDIVIDUAL_FILTERED_V1
        report_tab = body['report_tab']
        users_report_data_2 = ''
        users_report_data_3 = ''
        users_report_data_4 = ''
        message_by_language = str(language_id) + "_MESSAGES"
    except:
        # returning the error when there is some error in above try block
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)
    
    try:
        ##########  PHASE 1  ##########
        
        # 1) i) Reading the input file having mean, sd and regression function of clusters of clusters
        theme_num = list(range(1, 15))

        # Define section values
        section_num = list(range(1, 6))
        
        # getting list for the mean sd for each theme
        list_mean_sd_reg_fun = get_multiple_inputs(theme_num, MEAN_SD_REG_FUN)
        
        # Intialize list to store the percentile score for each theme 
        list_PT_percentile_score = []
        
        # 1) ii) Loading the csv 'cluster_personality_types_output.csv' containing consumed
        #        personality types and cluster of cluster percentile scores
        if CLUSTER_PT_OUTPUT_OR_PT_PERCENTILE_SCORES == 2:
            # Intialize list to store the percentile score for each theme 
            list_PT_percentile_score = get_multiple_inputs(theme_num, PERCENTILE_SCORES)
        
        # 1) iii) Loading the csv containing list of selected users to generate reports
        # Read the provided data input csv file having styles details for report section
        styles_report_df = pd.read_csv(data_file_name)
        
        #2) Retrieve the user's input data from the MySQL database
        try:
            # Create a connection with MySQL database.
            db_connection = make_connection()
            try:
                # Retrieve the user's input data from the MySQL database
                users_report_df = retrieve_users_report(user_id, db_connection)
                try:
                    users_report_df.iloc[0,1:]
                    gender_id = users_report_df['gender'].iloc[0]
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
        user_clust_predscore_df_list = predicted_score_across_clusters(list_mean_sd_reg_fun, users_report_df, theme_num)
        
        # 5) Generating the users percentile/z-score for each cluster of cluster
        user_clust_zscore_df_list = percentile_score_across_clusters(user_clust_predscore_df_list, list_mean_sd_reg_fun ,theme_num)
        
        # 6 and 7) Pre and post processing and function call for Assigning users to each styles
        user_theme_style_angle_df = processing_to_assign_user_to_style(user_clust_zscore_df_list, list_PT_percentile_score, theme_num)


        try:
            theme_value = user_theme_style_angle_df.loc[[13],['theme']].values[0]
            style_value  = user_theme_style_angle_df.loc[[13],['style']].values[0]
            value1 = str(int(theme_value))
            value2 = str(int(style_value))
            style_code_for_section_overview = value1+"-"+value2
        except:
            logger.error(traceback.format_exc())

        
        ##########  PHASE 2  ##########
        
        # 8) Function to generate individual user report
        list_user_reports = generate_style_summary(0, user_theme_style_angle_df, styles_report_df)
        
        # resetting the index after sorting
        list_user_reports = list_user_reports.reset_index(drop=True)

        extended_report_content_data_df = pd.DataFrame()
        extended_report_content_df = pd.DataFrame()
        extended_report_content_data = pd.DataFrame()

        if(report_tab == 'INDIVIDUAL_FILTERED_V1'):

            #Code for summary report data
            if((self_user == 0) & (section_id == 0)):
                available_summaries = get_summaries_report_content(list_user_reports)
                users_report_data_3 = [{"Rows" : "","Number": i[0], "Section" : "0", "Word_count" : "", "Illustration" : "", "Title" : "", "Story" : "0","Content":i[3]} for i in available_summaries.values.tolist()]
            else:
                #Code for summary report data
                if(self_user == 0):
                    available_summaries = get_summaries_report_content(list_user_reports)
                    users_report_data_3 = [{"Rows" : "","Number": i[0], "Section" : "0", "Word_count" : "", "Illustration" : "", "Title" : "", "Story" : "0","Content":i[3]} for i in available_summaries.values.tolist()]
                is_filtering_required = 'false'
                SECTION_TARGET_DICT = {"SECTION_TARGET_1": 600, "SECTION_TARGET_2": 900,"SECTION_TARGET_3": 600, "SECTION_TARGET_4": 500, "SECTION_TARGET_5": 500}
                
                for section_number in section_num:
                    section_wise_list = list_user_reports.loc[(list_user_reports['Section']==int(section_number))]
                    SECTION_TARGET_WORD_COUNT = SECTION_TARGET_DICT["SECTION_TARGET_"+str(section_number)]                
                    if(section_wise_list['word_count'].sum() >= SECTION_TARGET_WORD_COUNT):
                        is_filtering_required = 'true'
                        break

                # list_user_reports_df = pd.DataFrame()                               
                if(is_filtering_required == 'true'):
                    extended_report_content_data, list_user_reports = filtering_current_report_content(list_user_reports,extended_report_content_df,theme_num,section_num)
                    if(extended_report_content_data.empty):
                        extended_report_content_data, list_user_reports = get_extended_report_content(list_user_reports,section_num)
                else:
                    extended_report_content_data, list_user_reports = get_extended_report_content(list_user_reports,section_num)
                # removing the unwanted columns from the dataframe
                list_user_reports.drop(["word_count","Illustration", "Title"], axis = 1, inplace = True) 
        else:
            # removing the unwanted columns from the dataframe
            list_user_reports.drop(["word_count","Illustration", "Title"], axis = 1, inplace = True)
        
        ##########  PHASE 3  ##########
        
        # 1) ii) Loading the csv files having DAG data
        DAG_section_data_list = get_multiple_inputs(list(range(1,SECTIONS+1)), DAG_FILE)
        
        for i in DAG_section_data_list:
            i.drop(i.columns[[1]], axis=1,inplace=True)
            
        # 2) Sorting the origibal DAG as per user's styles: Reduced DAG
        user_profile_df = reduce_DAG(list_user_reports, DAG_section_data_list, styles_report_df, user_id)
        if(report_tab == 'INDIVIDUAL_FILTERED_V1'):
            if(extended_report_content_data.empty):
                print("Empty extended_report_content_data DataFrame")
            else:
                extended_report_content_data_df = reduce_DAG(extended_report_content_data, DAG_section_data_list, styles_report_df, user_id)
                # adding a column Rows to the dataframe
                extended_report_content_data_df.insert(0, "Rows", list(range(1,len(extended_report_content_data_df.axes[0])+1)), True)
        
        # adding a column Rows to the dataframe
        user_profile_df.insert(0, "Rows", list(range(1,len(user_profile_df.axes[0])+1)), True)        

        #Share report will show for self user
        if((report_tab == 'INDIVIDUAL_FILTERED_V1') & (self_user == 1)):
            #Read Share Modules csv file.    
            share_module_user = pd.read_csv(DATA_FILE_SHARE_MODULES)

            share_module_report_df = pd.DataFrame()

            #For extract share module data
            for section_number in section_num:
                module_report_dataframe = pd.DataFrame()
                for theme_number in theme_num:
                    if(module_report_dataframe.empty):
                        section_and_theme_wise_user_profile_df = user_profile_df[(user_profile_df['Section']==int(section_number)) & (user_profile_df['Theme']==int(theme_number))]
                        section_and_theme_wise_share_module_df = share_module_user[(share_module_user['section_id']==int(section_number)) & (share_module_user['theme_id']==int(theme_number))]
                        module_report_dataframe = section_and_theme_wise_share_module_df[(section_and_theme_wise_share_module_df.style_id.isin(section_and_theme_wise_user_profile_df.Style))]               
                if(module_report_dataframe.empty):
                    module_report_dataframe = share_module_user[share_module_user.section_id.isin(user_profile_df.Section) 
                    & (share_module_user['theme_id']==int(-1)) & (share_module_user['style_id']==int(-1))
                    & (share_module_user['section_id']==int(section_number))]

                #Add extended content data
                share_module_report_df = share_module_report_df.append(module_report_dataframe.iloc[:1])
                     
            users_report_data_4 = []
            for i in share_module_report_df.values:
                if int(i[2]) == int(language_id):
                    data = {"Rows" : "","Number": "", "Section" : i[0], "Word_count" : "", "Illustration" : "", "Title" : "", "Story" : "","Content":i[4]}
                else:
                    data = {"Rows": "", "Number": "", "Section": i[0], "Word_count": "", "Illustration": "", "Title": "", "Story": "", "Content": "BLANK"}                
                users_report_data_4.append(data)


        #CHANGES FOR TEST INTERFACE
        # if the section id is 0 then we have to select all the rows in the dataframe
        if section_id != -1:
            # filtering the data according to the section id  
            user_profile_df = user_profile_df.loc[user_profile_df['Section'].isin([section_id])]
            if(report_tab == 'INDIVIDUAL_FILTERED_V1'):
                if(extended_report_content_data.empty):
                    print("Empty extended_report_content_data DataFrame")
                else:
                    extended_report_content_data_df = extended_report_content_data_df.loc[extended_report_content_data_df['Section'].isin([section_id])]    

        # Formating the resulted report into the particular order
        users_report_data_1 = [{"Rows" : i[0],"Number": i[2], "Section" : i[5], "Word_count" : i[6], "Illustration" : i[7], "Title" : i[9], "Story" : i[11]} for i in user_profile_df.values.tolist()]

        # Add Summary report for other users
        if((report_tab == 'INDIVIDUAL_FILTERED_V1') & (self_user == 0)):
            users_report_data_1 = users_report_data_3 + users_report_data_1
        
        #Extended Report will show for self user
        if((report_tab == 'INDIVIDUAL_FILTERED_V1') & (self_user == 1)):
            users_report_data_2 = [{"Rows" : i[0],"Number": i[2], "Section" : i[5], "Word_count" : i[6], "Illustration" : i[7], "Title" : i[9], "Story" : i[11]} for i in extended_report_content_data_df.values.tolist()]

        r = json.dumps({"users_report_data_1" : users_report_data_1,"users_report_data_2" : users_report_data_2})
       
        resp = json.loads(r)

        try:
            # Making the DB connection
            cnx = make_connection_interpersonality_dbserver()
            # Getting the cursor from the DB connection to execute the queries
            cursor = cnx.cursor()
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)

        try :            
            theme_14_section_list = []            

            if(gender_id=='female'):
                gender_id_value = 1
            else:
                gender_id_value = 0

            json_result_list_1 = []
            json_result_list_2 = []
            for j in range(len(resp)):
                json_result_list = []
                number_list = []
                section_list = []
                data = resp['users_report_data_'+str(j+1)]

                lengthValues = len(resp['users_report_data_'+str(j+1)])

                for i in range(lengthValues):
                    rows = str(data[i]['Rows'])
                    number = str(data[i]['Number'])
                    section = str(data[i]['Section'])
                    wordCount = str(data[i]['Word_count'])
                    illustration = str(data[i]['Illustration'])
                    title = str(data[i]['Title'])
                    story = str(data[i]['Story'])
                    style_id = number[-2:]
                    theme_id = number.replace(style_id, "")
                    if section not in section_list:
                        number_list = []
                        section_list.append(section)

                    try:
                        # Executing the Query
                        if(section_id == -1):
                            try:
                                if section not in theme_14_section_list :
                                    theme_14_section_list.append(section)
                                    # Query for getting overview data
                                    selectionQuery = "SELECT `overview` FROM `section_overview` WHERE `theme_id`=%s AND `style_id`=%s  AND `language_id`=%s AND `gender_id`=%s AND `self_user`=%s  AND `section_id`=%s"
                                    
                                    themeId = ''
                                    styleId = ''

                                    themeId = style_code_for_section_overview.split('-')[0]
                                    styleId = style_code_for_section_overview.split('-')[1]                                
                                    styleId = styleId.zfill(2)

                                    cursor.execute(selectionQuery, (themeId,styleId,language_id,gender_id_value,self_user,section))
                                    result_list = []
                                    # fetching result from the cursor
                                    for result in cursor: result_list.append(result)
                                    if(result_list != []):
                                        json_result = {"Rows" : "","Number": themeId+styleId, "Section" : section, "Word_count" : "", "Illustration" : "", "Title" : "", "Story" : "0","Content":result_list[0][0]}
                                        if str(json_result['Number']) not in number_list:
                                            json_result_list.append(json_result)
                                            number_list.append(str(json_result['Number']))
                            except:
                                logger.error(traceback.format_exc())

                            # Query for getting content data
                            selectionQuery = "SELECT `content` FROM `profile_content` WHERE `theme_id`=%s AND `style_id`=%s AND `language_id`=%s AND `self_user`=%s AND `gender_id`=%s AND `section_id`=%s"
                            cursor.execute(selectionQuery, (theme_id,style_id,language_id,self_user,gender_id_value,section))                            
                        else:
                            try:
                                if section_id not in theme_14_section_list :
                                    theme_14_section_list.append(section_id)
                                    # Query for getting overview data
                                    selectionQuery = "SELECT `overview` FROM `section_overview` WHERE `theme_id`=%s AND `style_id`=%s AND `language_id`=%s AND `gender_id`=%s AND `self_user`=%s AND `section_id`=%s"
                                    
                                    themeId = ''
                                    styleId = ''

                                    themeId = style_code_for_section_overview.split('-')[0]
                                    styleId = style_code_for_section_overview.split('-')[1]                                
                                    styleId = styleId.zfill(2)

                                    cursor.execute(selectionQuery,(themeId,styleId,language_id,gender_id_value,self_user,section_id))
                                    result_list = []
                                    # fetching result from the cursor
                                    for result in cursor: result_list.append(result)                                
                                    if(result_list != []):
                                        json_result = {"Rows" : "","Number": themeId+styleId, "Section" : section_id, "Word_count" : "", "Illustration" : "", "Title" : "", "Story" : "0","Content":result_list[0][0]}
                                        if str(json_result['Number']) not in number_list:
                                            json_result_list.append(json_result)
                                            number_list.append(str(json_result['Number'])) 
                            except:
                                logger.error(traceback.format_exc())                            
                            # Query for getting content data
                            selectionQuery = "SELECT `content` FROM `profile_content` WHERE `theme_id`=%s AND `style_id`=%s AND `language_id`=%s AND `self_user`=%s AND `gender_id`=%s AND `section_id`=%s"
                            cursor.execute(selectionQuery, (theme_id,style_id,language_id,self_user,gender_id_value,section_id))                            
                        
                        result_list = []
                        # fetching result from the cursor
                        for result in cursor: result_list.append(result)
                        # getting content data                        
                        if(result_list != []):
                            content = result_list[0][0]
                            json_result = {"Rows" : rows,"Number": number, "Section" : section, "Word_count" : wordCount, "Illustration" : illustration, "Title" : title, "Story" : story,"Content":content}
                            if str(json_result['Number']) not in number_list:
                                            json_result_list.append(json_result)
                                            number_list.append(str(json_result['Number']))                       
                        else:
                            json_result = {"Rows" : rows,"Number": number, "Section" : section, "Word_count" : wordCount, "Illustration" : illustration, "Title" : title, "Story" : story,"Content":"BLANK"}
                            if str(json_result['Number']) not in number_list:
                                            json_result_list.append(json_result)
                                            number_list.append(str(json_result['Number']))
                    except:
                        # If there is any error in above operations, logging the error
                        logger.error(traceback.format_exc())          
                if(j==0):
                    json_result_list_1 = json_result_list
                else:      
                    json_result_list_2 = json_result_list                                                                                  
        finally:
            try:
                # Finally, clean up the connection
                cursor.close()
                cnx.close()
            except:
                pass        

        # returning success json
        return {
                    'statusCode': 200,
                    'headers' : {
                                    'Access-Control-Allow-Origin': '*',
                                    'Access-Control-Allow-Credentials': 'true'
                                },
                    'body': json.dumps({"current_report_content" : json_result_list_1,"extended_report_content" : json_result_list_2,"share_module_report_content" : users_report_data_4})
                }
    except:
        # returning the error when there is some error in above try block
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)

if __name__== "__main__":
    handler(None,None)
