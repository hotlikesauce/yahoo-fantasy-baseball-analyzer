import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
from functools import reduce
from pymongo import MongoClient
import certifi
import numpy as np
import os, logging, traceback, sys
from dotenv import load_dotenv
from sklearn.preprocessing import MinMaxScaler

# Local Modules - email utils for failure emails, mongo utils to 
from email_utils import send_failure_email
from manager_dict import manager_dict
from mongo_utils import *
from datetime_utils import *

from yahoo_utils import *

# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')
MONGO_DB = os.environ.get('MONGO_DB')

logging.basicConfig(filename='error.log', level=logging.ERROR)

def get_initial_elo():

    data = {
    'Team_Number': list(range(1, 13)),
    'ELO_Sum': [0.9] * 12,
    'Week': [1] * 12
    }

    # Creating the DataFrame
    initial_elo_df = pd.DataFrame(data)

    return initial_elo_df

def expected_outcome(elo_df,schedule_df):

    # Convert the nested values to their regular representation
    def convert_nested_values(value):
        if isinstance(value, dict) and '$numberInt' in value:
            return int(value['$numberInt'])
        return value

    # Apply the conversion function to each cell in the DataFrame
    schedule_df = schedule_df.applymap(convert_nested_values)

    try:
        elo_df.drop(columns=['Expected_Result_Ra', 'Normalized_Score_Difference'], inplace=True)
        elo_df.drop(columns=['ELO_Team_Sum'], inplace=True)
        elo_df.rename(columns={'New_ELO': 'ELO_Team_Sum'}, inplace=True)
        elo_df.rename(columns={'ELO_Sum': 'ELO_Team_Sum'}, inplace=True)
    except KeyError:
        pass

    # Select the desired columns from elo_df
    print('ELO DF IN EXPECTED OUTCOME')
    print('-----------------------------------------------')
    print(elo_df)

    elo_subset = elo_df[['Team_Number', 'ELO_Team_Sum', 'Week']]

    # Selecting the desired columns from 'schedule_df'
    schedule_subset = schedule_df[['Opponent_Team_Number', 'Week']]
    try:
        schedule_subset.rename(columns={'Week': 'Next_Week'}, inplace=True)
    except KeyError:
        pass

    # Creating a new DataFrame with the specified columns and values
    joined_df = pd.concat([elo_subset, schedule_subset], axis=1)
    joined_df = joined_df.dropna()


    print(joined_df)
    
    # Convert the data type of 'Opponent_Team_Number' to int64
    joined_df['Opponent_Team_Number'] = joined_df['Opponent_Team_Number'].astype('int64')

    # Merge the DataFrames on 'Opponent_Team_Number' and 'Team_Number'
    # Convert the data type of 'Opponent_Team_Number' to match 'Team_Number'
    joined_df['Opponent_Team_Number'] = joined_df['Opponent_Team_Number'].astype(elo_subset['Team_Number'].dtype)


    joined_df['Team_Number'] = joined_df['Team_Number'].astype('int64')
    joined_df['Opponent_Team_Number'] = joined_df['Opponent_Team_Number'].astype('int64')

    elo_subset['Team_Number'] = elo_subset['Team_Number'].astype('int64')

    # Merge the DataFrames on 'Opponent_Team_Number' and 'Team_Number'
    joined_df = pd.merge(joined_df, elo_subset[['Team_Number', 'ELO_Team_Sum']], left_on='Opponent_Team_Number', right_on='Team_Number', how='left')
    
    # Rename the ELO_Team_Sum column to a desired name (e.g., ELO_Opponent_Sum)
    joined_df.rename(columns={'ELO_Team_Sum': 'ELO_Opponent_Sum'}, inplace=True)

    # Drop the additional Team_Number column
    joined_df.drop('Team_Number_y', axis=1, inplace=True)

    # Rename the columns in joined_df
    joined_df.rename(columns={'Team_Number_x': 'Team_Number', 'ELO_Team_Sum_x': 'ELO_Team_Sum', 'ELO_Team_Sum_y': 'ELO_Opponent_Sum'}, inplace=True)

    joined_df['Expected_Result_Ra'] = 1 / (1 + 25 ** ((joined_df['ELO_Opponent_Sum'] - joined_df['ELO_Team_Sum']) / 400))

    

    return joined_df

def get_new_elo(expected_outcome_df,week_df):

    #DUPLICATE TEAM_NUMBER COLS - NEED TO REDIFE COL FROM OBJ TO INT
    week_df['Team_Number'] = week_df['Team_Number'].astype('int64')
    merged_df = expected_outcome_df.merge(week_df[['Normalized_Score_Difference', 'Team_Number']], on=['Team_Number'], how='left')

    print(merged_df)

    K_Factor = 50

    appended_df = pd.DataFrame() 


    for index, row in merged_df.iterrows():
        week = row['Week']
        team_number = row['Team_Number']
        elo_team_sum = row['ELO_Team_Sum']
        Expected_Result_Ra = (row['Expected_Result_Ra']-.5)*2
        Normalized_Score_Difference = (row['Normalized_Score_Difference']-.5)*2

        #scaled_expected = sigmoid(Expected_Result_Ra*10)
        #scaled_actual = sigmoid(Normalized_Score_Difference*10)


        New_ELO = (elo_team_sum + K_Factor*(Normalized_Score_Difference)-Expected_Result_Ra)

        appended_row = pd.DataFrame({
            'Week': [week+1],
            'Team_Number': [team_number],
            'ELO_Team_Sum': [elo_team_sum],
            'Expected_Result_Ra': [Expected_Result_Ra],
            'Normalized_Score_Difference': [Normalized_Score_Difference],
            'New_ELO': [New_ELO]
        })

        # Append the row DataFrame to the appended_df DataFrame
        appended_df = pd.concat([appended_df, appended_row], ignore_index=True)

    print(appended_df)
    return appended_df
   
def get_week_6_elo(week_6_df):

    # Columns to analyze
    high_columns_to_analyze = ['R_Stats', 'H_Stats', 'HR_Stats', 'RBI_Stats', 'SB_Stats', 'OPS_Stats','K9_Stats', 'QS_Stats', 'SVH_Stats' ]

    low_columns_to_analyze = ['ERA_Stats', 'WHIP_Stats', 'HRA_Stats']

    # Calculate ELO for each column grouped by team_number
    for column in high_columns_to_analyze:
        min_elo = 75  # Set the desired minimum ELO value
        min_value = week_6_df[column].min()
        max_value = week_6_df[column].max()
        
        scaler = MinMaxScaler(feature_range=(min_elo, 100))
        
        # Calculate and assign the scaled ELO values
        week_6_df[column + '_ELO'] = scaler.fit_transform(week_6_df[column].values.reshape(-1, 1))    
    
    # Calculate ELO for each LOW column grouped by team_number
    for column in low_columns_to_analyze:
        min_elo = 50  # Set the desired minimum ELO value
        min_value = week_6_df[column].min()
        max_value = week_6_df[column].max()
        
        scaler = MinMaxScaler(feature_range=(min_elo, 100))
        
        # Calculate and assign the scaled ELO values
        scaled_values = 100 - ((week_6_df[column] - min_value) / (max_value - min_value)) * 80
        week_6_df[column + '_ELO'] = scaled_values

    # Get the list of ELO columns
    elo_columns = [column + '_ELO' for column in high_columns_to_analyze + low_columns_to_analyze]

    # Sum the ELO columns
    week_6_df['ELO_Sum'] = week_6_df[elo_columns].sum(axis=1)

    return week_6_df


def main():
    try:
        # empty_dict = {}
        # week_6_dict = {"Week":3}
        # power_rank_df = get_mongo_data('power_ranks',empty_dict)
        # power_rank_season_df = get_mongo_data('power_ranks_season_trend',week_6_dict)
        #schedule_df = get_mongo_data('schedule',empty_dict)


        ## --------------- This is more for weighted stats than ELO will revisit---------------------- ##
        
        this_week = set_this_week()
        # week_6_elo_df = get_week_6_elo(power_rank_season_df)
        # print(week_6_elo_df)

        data = {
            'Team_Number': list(range(1, 13)),
            'ELO_Team_Sum': [1000] * 12,
            'Week': [1] * 12
        }

        # Create the DataFrame
        week_1_df = pd.DataFrame(data)

        running_elo_df = pd.DataFrame()
        output_df = week_1_df
        #output_df = week_6_elo_df
        
        
        for week in range(1,(this_week)):
            week_dict =  {"Week":week}
            schedule_df = get_mongo_data(MONGO_DB,'schedule',week_dict)
            print(schedule_df)
            expected_outcome_df = expected_outcome(output_df,schedule_df)
            print(expected_outcome_df)

            week_dict =  {"Week":(week)}
            week_df = get_mongo_data(MONGO_DB,'weekly_results', week_dict)

           
            output_df = get_new_elo(expected_outcome_df, week_df)
            

            running_elo_df = running_elo_df.append(output_df, ignore_index=True)

        week_1_df = week_1_df.rename(columns={'ELO_Team_Sum': 'New_ELO'})
        running_elo_df = running_elo_df.append(week_1_df, ignore_index=True)
        running_elo_df['Team_Number'] = running_elo_df['Team_Number'].astype(int).astype(str).replace('\.0', '', regex=True)
        print(running_elo_df)
        clear_mongo(MONGO_DB,'Running_ELO')
        #clear_mongo_query('Running_ELO','"Week":13')
        write_mongo(MONGO_DB,running_elo_df,'Running_ELO')

    except Exception as e:
        filename = os.path.basename(__file__)
        line_number = traceback.extract_tb(sys.exc_info()[2])[-1][1]
        error_message = str(e)
        additional_info = f'Error occurred at line {line_number}'
        logging.error(f'{filename}: {error_message} - {additional_info}')
        raise  # Raising the exception again to propagate it
        #send_failure_email(error_message,filename)

if __name__ == '__main__':
    main()
