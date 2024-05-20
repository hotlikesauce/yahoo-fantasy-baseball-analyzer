import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
from functools import reduce
from pymongo import MongoClient
import certifi
import os,sys
from dotenv import load_dotenv
from sklearn.preprocessing import MinMaxScaler
import warnings
# Ignore the FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

# Local Modules - email utils for failure emails, mongo utils to 
from email_utils import send_failure_email
from manager_dict import manager_dict
from mongo_utils import *
from datetime_utils import *
from yahoo_utils import *
from categories_dict import Low_Categories


# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')
MONGO_DB = os.environ.get('MONGO_DB')


def get_normalized_ranks(all_time_rank_df):
    print(all_time_rank_df)
    #parse through columns and figure out which ones are low-based
    low_columns_to_analyze = []
    high_columns_to_analyze = []

    for column in all_time_rank_df.columns:
        if column == 'Team' or column == 'Opponent' or column == '_id':
            pass
        elif column in Low_Categories:
            low_columns_to_analyze.append(column)
        else:
            high_columns_to_analyze.append(column)
    else:
        pass
    # Calculate Score for each column grouped by team_number
    
    print(low_columns_to_analyze)
    print(high_columns_to_analyze)

    for column in high_columns_to_analyze:
        min_score = 0  # Set the desired minimum Score value
        min_value = all_time_rank_df[column].min()
        max_value = all_time_rank_df[column].max()
        
        scaler = MinMaxScaler(feature_range=(min_score, 100))
        
        # Calculate and assign the scaled Score values
        all_time_rank_df[column + '_Score'] = scaler.fit_transform(all_time_rank_df[column].values.reshape(-1, 1))    
    
    # Calculate Score for each LOW column grouped by team_number
    for column in low_columns_to_analyze:
        min_score = 0  # Set the desired minimum Score value
        min_value = all_time_rank_df[column].min()
        max_value = all_time_rank_df[column].max()
        
        scaler = MinMaxScaler(feature_range=(min_score, 100))
        
        # Calculate and assign the scaled Score values
        scaled_values = 100 - ((all_time_rank_df[column] - min_value) / (max_value - min_value)) * 100
        all_time_rank_df[column + '_Score'] = scaled_values

    # Get the list of Score columns
    score_columns = [column + '_Score' for column in high_columns_to_analyze + low_columns_to_analyze]

    # Sum the Score columns
    all_time_rank_df['Score_Sum'] = all_time_rank_df[score_columns].sum(axis=1)
    all_time_rank_df['Score_Rank'] = all_time_rank_df['Score_Sum'].rank(ascending=False)
    all_time_rank_df = build_team_numbers(all_time_rank_df)  

    print(all_time_rank_df)
    return all_time_rank_df

def main():
    num_teams = league_size()
    leaguedf = league_stats_all_df()
    lastWeek = set_last_week()
    try:
        running_df = pd.DataFrame()
    
        for week in range(1,2):
            weeks_of_interest = [week]


            #Generate ranks and running ranks in lieu of running power ranks which started at the beginning of the season
            for weeks in weeks_of_interest:
                weekly_stats_df = get_mongo_data(MONGO_DB,'weekly_stats','"Week": '+str(weeks))
                running_df = pd.concat([running_df, weekly_stats_df], ignore_index=True)
            aggregations = {
                'R': 'sum', 'H': 'sum', 'HR': 'sum', 'RBI': 'sum', 'SB': 'sum',
                'HRA': 'sum', 'QS': 'sum', 'SVH': 'sum',
                'OPS': 'mean', 'ERA': 'mean', 'WHIP': 'mean', 'K9': 'mean'
            }

            # Group by 'Team' and aggregate
            team_stats = running_df.groupby('Team').agg(aggregations).reset_index()

            print(team_stats)
            normalized_ranks_df = get_normalized_ranks(team_stats)
            normalized_ranks_df['Week'] = week
            print(normalized_ranks_df)
            #clear_mongo_query(MONGO_DB,'running_normalized_ranks','"Week":'+str(week))
            #write_mongo(MONGO_DB,normalized_ranks_df,'running_normalized_ranks')

    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        send_failure_email(error_message, filename)

if __name__ == '__main__':
    main()
