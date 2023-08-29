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
# Custom function to convert values in the 'Week' column
def convert_to_int(x):
    if isinstance(x, dict):
        return int(x.get('$numberInt', 0))
    return int(x)

def main():
    try:
        this_week = set_this_week()
        if this_week == 21:
            print("No matchups next week, playoffs")
            return
        # Convert the values in the 'week' column to integers
        schedule_df = get_mongo_data(MONGO_DB,'schedule','')
        schedule_df['Week'] = schedule_df['Week'].apply(convert_to_int)

        print(schedule_df)
        filtered_schedule_df = schedule_df[schedule_df['Week'] > this_week]
        
        
        power_ranks_df = get_mongo_data(MONGO_DB,'normalized_ranks','')
        print(power_ranks_df)
        
        # Merge the filtered_schedule_df and power_ranks_df on 'opponent_team_number' and 'team_number' respectively
        merged_df = filtered_schedule_df.merge(power_ranks_df, left_on='Opponent_Team_Number', right_on='Team_Number', suffixes=('_schedule', '_ranks'))
        print(merged_df)

        # Group by 'Team_Number' in filtered_schedule_df and calculate the sum of 'score_sum' from power_ranks_df for each group
        grouped_df = merged_df.groupby('Team_Number_schedule', as_index=False)['Score_Sum'].sum()

        # Rename the 'score_sum' column to a more descriptive name, e.g., 'total_score_sum'
        grouped_df.rename(columns={'Score_Sum': 'total_score_sum'}, inplace=True)
        grouped_df.rename(columns={'Team_Number_schedule': 'Team_Number'}, inplace=True)

        # Merge the grouped_df back to the filtered_schedule_df on 'Team_Number'
        grouped_df.rename(columns={'total_score_sum': 'Opponent_Sum_Power_Rank'}, inplace=True)
 
        # Calculate the 'Avg_Power_Rank' column
        max_week = schedule_df['Week'].max()

        grouped_df['Avg_Power_Rank'] = grouped_df['Opponent_Sum_Power_Rank'] / (max_week - this_week)

        # Sort the DataFrame by 'Opponent_Sum_Power_Rank' in ascending order
        grouped_df.sort_values(by='Opponent_Sum_Power_Rank', ascending=False, inplace=True)

        # Add the 'Rank' column based on the sorting order
        grouped_df['SOS_Rank'] = range(1, len(grouped_df) + 1)


        # Now, 'final_df' contains the new column 'total_score_sum' which has the sum of 'score_sum' from power_ranks_df
        print(grouped_df)
        clear_mongo(MONGO_DB,'remaining_sos')
        write_mongo(MONGO_DB,grouped_df,'remaining_sos')
    

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


