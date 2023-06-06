import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
from pymongo import MongoClient
import time, datetime, os
import certifi
from dotenv import load_dotenv

# Local Modules - email utils for failure emails, mongo utils to 
from email_utils import send_failure_email
from datetime_utils import set_this_week
from manager_dict import manager_dict
from mongo_utils import *
from yahoo_utils import build_team_numbers

# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')
this_week = set_this_week()


def last_four_weeks_coefficient():
    # Connect to the MongoDB server
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)

    # Access the database and collection
    db = client['YahooFantasyBaseball_2023']
    collection = db['coefficient']

    # Retrieve the data from the collection
    data = list(collection.find())

    # Convert the data to a pandas DataFrame
    last_four_weeks_df = pd.DataFrame(data)

    # Filter the DataFrame based on the condition
    last_four_weeks_df = last_four_weeks_df[last_four_weeks_df['Week'] >= this_week - 4]

    # You can now work with the filtered DataFrame
    print(last_four_weeks_df)

def last_four_weeks(matchups_df):
    
    #Set week number
    last_four_weeks_stats = pd.DataFrame(columns = ['Team','Week','R','H','HR','SB','OPS','RBI','HRA','ERA','WHIP','K9','QS','SVH'])
    for week in range(this_week-4,this_week):
        for matchup in range(1,13):
            data=[]      
            #Setting this sleep timer on a few weeks helps with the rapid requests to the Yahoo servers
            #If you request the site too much in a short amount of time you will be blocked temporarily
            time.sleep(1)     
            
            def open_url(url):
                try:
                    source = urllib.request.urlopen(url).read()
                    return source
                except urllib.error.HTTPError as e:
                    if e.code == 404:
                        print("Error 404: Page not found. Retrying...")
                        return open_url(url)  # Recursive call to retry opening the URL
                    else:
                        raise e  # Reraise any other HTTP errors
            try:            
                source = urllib.request.urlopen(YAHOO_LEAGUE_ID+'matchup?week='+str(week)+'&module=matchup&mid1='+str(matchup)).read()
                soup = bs.BeautifulSoup(source,'lxml')
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    print("Error 404: Page not found. Retrying...")
                    print(YAHOO_LEAGUE_ID+'matchup?week='+str(week)+'&module=matchup&mid1='+str(matchup))
                    source = urllib.request.urlopen(YAHOO_LEAGUE_ID+'matchup?week='+str(week)+'&module=matchup&mid1='+str(matchup)).read()
                else:
                    raise e


            table = soup.find_all('table')
            df = pd.read_html(str(table))[1]
            df['Week'] = week
            df.columns = df.columns.str.replace('[#,@,&,/,+]', '')
            #RENAME COLUMN FOR HR ALLOWED SINCE IT'S A DUPLICATE OF THE BATTING CATEGORY
            df.rename(columns={df.columns[9]: 'HRA'},inplace=True)
            
            # Logic below to handle asterisks that happen for % based stats when ties occur
            df['WHIP'] = df['WHIP'].astype(str)
            df['OPS'] = df['OPS'].astype(str)
            df['K9'] = df['K9'].astype(str)
            

            df['WHIP'] = df['WHIP'].map(lambda x: x.rstrip('*'))
            df['OPS'] = df['OPS'].map(lambda x: x.rstrip('*'))
            df['K9'] = df['K9'].map(lambda x: x.rstrip('*'))

            df['WHIP'] = df['WHIP'].replace(['-'],'0.00')
            df['OPS'] = df['OPS'].replace(['-'],'0.00')
            df['K9'] = df['K9'].replace(['-'],'0.00')

            df['WHIP'] = df['WHIP'].astype(float)
            df['OPS'] = df['OPS'].astype(float)
            df['K9'] = df['K9'].astype(float)


            df=df[['Team','Week','R','H','HR','SB','OPS','RBI','HRA','ERA','WHIP','K9','QS','SVH']]
            #df = df.reset_index()
            #print(df)
            #print(df.loc[1,'Team'])
            
            

            last_four_weeks_stats = last_four_weeks_stats.append(df.loc[0], True)   
    
    cols_to_average = ['R', 'H', 'HR', 'RBI', 'OPS', 'SB', 'HRA', 'QS', 'SVH','WHIP','ERA','K9']
    last_four_weeks_stats[cols_to_average] = last_four_weeks_stats[cols_to_average].astype(float)
    averages = last_four_weeks_stats.groupby('Team')[cols_to_average].mean().reset_index()
    last_four_weeks_avg = last_four_weeks_stats.merge(averages, on=['Team'], suffixes=('', '_Avg')) 
    last_four_weeks_avg = last_four_weeks_avg.drop_duplicates(subset='Team')
    cols_to_drop = ['Week', 'R', 'H', 'HR', 'SB', 'OPS', 'RBI', 'HRA', 'ERA', 'WHIP', 'K9', 'QS', 'SVH']

    # Drop the specified columns
    last_four_weeks_avg = last_four_weeks_avg.drop(cols_to_drop, axis=1)

    final_return_df = build_team_numbers(last_four_weeks_avg)
    final_return_df = final_return_df.merge(matchups_df[['Team_Number', 'Opponent_Team_Number']], on='Team_Number')


    print(final_return_df)

    return final_return_df

def get_matchups():
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)

    # Access the database and collection
    db = client['YahooFantasyBaseball_2023']
    collection = db['schedule']

    # Retrieve the data from the collection
    matchup_data = list(collection.find())

    # Convert the data to a pandas DataFrame
    matchups_df = pd.DataFrame(matchup_data)

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


    print(manager_dict_df)
    matchups_df = matchups_df.drop_duplicates(subset=['Week', 'Team_Number', 'Opponent_Team_Number'])
    matchups_df = matchups_df.drop('_id', axis=1)
    
    client.close()


    return matchups_df

def predict_matchups(last_four_weeks_stats_df):
    # Iterate over the rows of the DataFrame
    for index, row in last_four_weeks_stats_df.iterrows():
        # Get the team numbers and opponent team numbers
        team_num = row['Team_Number']
        opp_num = row['Opponent_Team_Number']
        print(f'{team_num} {opp_num}')
        
        # Find the rows that match the given team numbers
        team_row = last_four_weeks_stats_df.loc[last_four_weeks_stats_df['Team_Number'] == team_num]
        opp_row = last_four_weeks_stats_df.loc[last_four_weeks_stats_df['Team_Number'] == opp_num]
        
        # Exclude 'Team', 'Team_Number', and 'Opponent_Team_Number' from comparison
        high_cols_to_compare = ['R_Avg', 'H_Avg', 'HR_Avg', 'RBI_Avg', 'OPS_Avg', 'SB_Avg','QS_Avg', 'SVH_Avg','K9_Avg']
        
         # Compare the values for each column
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
        
        # Exclude 'Team', 'Team_Number', and 'Opponent_Team_Number' from comparison
        low_cols_to_compare = ['ERA_Avg', 'WHIP_Avg', 'HRA_Avg']
        
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

    print(result_df)
    return result_df



def main():
    try:
        clear_mongo('Weekly_Predictions_Records')
        clear_mongo('Weekly_Predictions_Stats')
        #last_four_weeks_coefficient_df = last_four_weeks_coefficient()
        matchups_df = get_matchups()
        last_four_weeks_avg = last_four_weeks(matchups_df)
        predictions_df = predict_matchups(last_four_weeks_avg)
        records_df = get_records(predictions_df)
        write_mongo(predictions_df,'Weekly_Predictions_Stats')
        write_mongo(records_df, 'Weekly_Predictions_Records')

    except Exception as e:
        filename = os.path.basename(__file__)
        error_message = str(e)
        send_failure_email(error_message,filename)

if __name__ == '__main__':
    main()
