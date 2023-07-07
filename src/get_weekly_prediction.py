import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
from pymongo import MongoClient
import time, datetime, os, sys
import certifi
from dotenv import load_dotenv
import warnings
# Ignore the FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

# Local Modules - email utils for failure emails, mongo utils to 
from email_utils import send_failure_email
from datetime_utils import set_this_week
from manager_dict import manager_dict
from mongo_utils import *
from yahoo_utils import *

# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')
MONGO_DB = os.environ.get('MONGO_DB')
this_week = set_this_week()


def last_four_weeks_coefficient(data):
    
    print(data)
    # Convert the data to a pandas DataFrame
    last_four_weeks_df = pd.DataFrame(data)

    # Filter the DataFrame based on the condition
    last_four_weeks_df = last_four_weeks_df[last_four_weeks_df['Week'] >= this_week - 4]

    print(last_four_weeks_df)

    # You can now work with the filtered DataFrame
    return(last_four_weeks_df)

def last_two_weeks_coefficient(data):
    
    # Filter the DataFrame based on the condition
    last_four_weeks_df = last_four_weeks_df[last_four_weeks_df['Week'] >= this_week - 2]

    # You can now work with the filtered DataFrame
    print(last_four_weeks_df)
    return(last_four_weeks_df)

def last_week_coefficient(data):
    # Filter the DataFrame based on the condition
    last_four_weeks_df = last_four_weeks_df[last_four_weeks_df['Week'] >= this_week - 1]

    # You can now work with the filtered DataFrame
    print(last_four_weeks_df)
    return(last_four_weeks_df)

def last_four_weeks(matchups_df):
    num_teams = league_size()
    this_week = set_this_week()
    leaguedf = league_stats_all_df()
    cols = leaguedf.columns.tolist()
    #Set week number
    last_four_weeks_stats = pd.DataFrame(columns = cols)
    for week in range(this_week-4,this_week):
        for matchup in range(1,(num_teams+1)):
            soup = url_requests(YAHOO_LEAGUE_ID+'matchup?week='+str(week)+'&module=matchup&mid1='+str(matchup))
            table = soup.find_all('table')
            df = pd.read_html(str(table))[1]
            df['Week'] = week
            df.columns = df.columns.str.replace('[#,@,&,/,+]', '')
            
            for column in df.columns:
                if column in percentage_categories:
                    # Logic below to handle asterisks that happen for % based stats when ties occur
                    df[column] = df[column].astype(str)  # Convert column to string type
                    # Remove asterisks from column values
                    df[column] = df[column].map(lambda x: x.rstrip('*'))
                    # Replace '-' with '0.00'
                    df[column] = df[column].replace(['-'], '0.00')
                    # Convert column to float type
                    df[column] = df[column].astype(float)  

            column_list = leaguedf.columns.tolist()
            df = df[column_list]
            last_four_weeks_stats = last_four_weeks_stats.append(df.loc[0], True)

    cols_to_average = []
    for column in last_four_weeks_stats:
        if column in all_categories:
            cols_to_average.append(column)
    
    last_four_weeks_stats[cols_to_average] = last_four_weeks_stats[cols_to_average].astype(float)
    averages = last_four_weeks_stats.groupby('Team')[cols_to_average].mean().reset_index()
    last_four_weeks_avg = last_four_weeks_stats.merge(averages, on=['Team'], suffixes=('', '_Avg')) 
    last_four_weeks_avg = last_four_weeks_avg.drop_duplicates(subset='Team')
    cols_to_average.insert(0, 'Week')
    cols_to_drop = cols_to_average

    # Drop the specified columns
    last_four_weeks_avg = last_four_weeks_avg.drop(columns=cols_to_drop)

    final_return_df = build_team_numbers(last_four_weeks_avg)
    final_return_df = final_return_df.merge(matchups_df[['Team_Number', 'Opponent_Team_Number']], on='Team_Number')
    return final_return_df

def get_matchups(matchups_df):

    # Convert the nested values to their regular representation
    def convert_nested_values(value):
        if isinstance(value, dict) and '$numberInt' in value:
            return int(value['$numberInt'])
        return value

    # Apply the conversion function to each cell in the DataFrame
    matchups_df = matchups_df.applymap(convert_nested_values)

    # Filter the DataFrame based on the condition
    matchups_df = matchups_df[matchups_df['Week'] == this_week]

    # Define column names
    columns = ['Manager_Number', 'Manager_Name']

    # Convert dictionary to DataFrame with specified columns
    manager_dict_df = pd.DataFrame(manager_dict.items(), columns=columns)

    matchups_df = matchups_df.drop_duplicates(subset=['Week', 'Team_Number', 'Opponent_Team_Number'])
    matchups_df = matchups_df.drop('_id', axis=1)
    



    return matchups_df

def predict_matchups(last_four_weeks_stats_df):
    # Iterate over the rows of the DataFrame

    high_cols_to_compare = [col for col in last_four_weeks_stats_df.columns if col not in Low_Categories_Avg and col not in ['Team', 'Team_Number','Opponent_Team_Number']]
    low_cols_to_compare = [col for col in last_four_weeks_stats_df.columns if col in Low_Categories_Avg]
        
    for index, row in last_four_weeks_stats_df.iterrows():
        # Get the team numbers and opponent team numbers
        team_num = row['Team_Number']
        opp_num = row['Opponent_Team_Number']
        print(f'{team_num} {opp_num}')
        
        # Find the rows that match the given team numbers
        team_row = last_four_weeks_stats_df.loc[last_four_weeks_stats_df['Team_Number'] == team_num]
        opp_row = last_four_weeks_stats_df.loc[last_four_weeks_stats_df['Team_Number'] == opp_num]
        
        #Compare the values for each column
        for col in high_cols_to_compare:
            team_value = team_row[col].values[0]
            opp_value = opp_row[col].values[0]
            
            # Add the 'WL' suffix to the column name
            col_wl = col + '_WL'
            
            # Compare the values and assign the corresponding values to the new column
            if team_value > opp_value:
                last_four_weeks_stats_df.at[index, col_wl] = 1
            elif team_value < opp_value:
                last_four_weeks_stats_df.at[index, col_wl] = 0
            else:
                last_four_weeks_stats_df.at[index, col_wl] = 0.5
        
                
        # Compare the values for each column
        for col in low_cols_to_compare:
            team_value = team_row[col].values[0]
            opp_value = opp_row[col].values[0]
            
            # Add the 'WL' suffix to the column name
            col_wl = col + '_WL'
            
            # Compare the values and assign the corresponding values to the new column
            if team_value < opp_value:
                last_four_weeks_stats_df.at[index, col_wl] = 1
            elif team_value > opp_value:
                last_four_weeks_stats_df.at[index, col_wl] = 0
            else:
                last_four_weeks_stats_df.at[index, col_wl] = 0.5
        

    print(last_four_weeks_stats_df)
    return last_four_weeks_stats_df

def get_records(last_four_weeks_stats_df):
    # Create a new DataFrame for the results
    result_df = pd.DataFrame(columns=['Team', 'Opponent', 'Win', 'Loss', 'Tie'])

    # Group the DataFrame by Team_Number
    grouped = last_four_weeks_stats_df.groupby('Team_Number')

    # Iterate over the groups
    for team_num, team_group in grouped:
        # Get the opponent team numbers for the current team
        opponent_nums = team_group['Opponent_Team_Number']
        
        # Find the rows that match the given team numbers
        team_rows = last_four_weeks_stats_df.loc[last_four_weeks_stats_df['Team_Number'] == team_num]
        opp_rows = last_four_weeks_stats_df.loc[last_four_weeks_stats_df['Team_Number'].isin(opponent_nums)]
        
        # Calculate the sum of '_WL' columns for the team
        win_count = team_group.filter(regex='_WL$').eq(1).sum().sum()
        loss_count = team_group.filter(regex='_WL$').eq(0).sum().sum()
        tie_count = team_group.filter(regex='_WL$').eq(0.5).sum().sum()
        
        # Get the team and opponent names
        team_name = team_rows['Team'].values[0]
        opponent_names = opp_rows['Team'].values
        
        # Create a new row with the results
        result_row = {'Team': team_name, 'Opponent': ', '.join(opponent_names),
                    'Win': win_count, 'Loss': loss_count, 'Tie': tie_count}
        
        # Append the row to the result DataFrame
        result_df = result_df.append(result_row, ignore_index=True)

    return result_df


def main():
    data = get_mongo_data(MONGO_DB,'coefficient','')
    matchup_data = get_mongo_data(MONGO_DB,'schedule','')
    try:
        this_week = set_this_week()
        #Get coefficient of last 4 weeks
        last_four_weeks_coefficient_df = last_four_weeks_coefficient(data)
        print(last_four_weeks_coefficient_df)
        clear_mongo(MONGO_DB,'Coefficient_Last_Four')
        write_mongo(MONGO_DB,last_four_weeks_coefficient_df, 'Coefficient_Last_Four')
        
        #Get matchups/schedule and last 4 weeks avg stats
        matchups_df = get_matchups(matchup_data)
        last_four_weeks_avg = last_four_weeks(matchups_df)
        print(last_four_weeks_avg)
        
        #compare based on matchups
        predictions_df = predict_matchups(last_four_weeks_avg)
        clear_mongo(MONGO_DB,'Weekly_Predictions_Stats')
        write_mongo(MONGO_DB,predictions_df,'Weekly_Predictions_Stats')

        #Get stats and predicted records
        records_df = get_records(predictions_df)
        clear_mongo(MONGO_DB,'Weekly_Predictions_Records')
        write_mongo(MONGO_DB,records_df, 'Weekly_Predictions_Records')


    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        send_failure_email(error_message, filename)

if __name__ == '__main__':
    main()
