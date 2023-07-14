import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
from pymongo import MongoClient
import time, datetime, os, sys
import certifi
from loguru import logger
from dotenv import load_dotenv
import warnings
# Ignore the FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

# Local Modules - email utils for failure emails, mongo utils to 
from email_utils import send_failure_email
from datetime_utils import *
from manager_dict import manager_dict
from yahoo_utils import *
from mongo_utils import *

# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')
MONGO_DB = os.environ.get('MONGO_DB')

def get_weekly_results(num_teams,max_week):
    #Set week number
    weekly_results_df = pd.DataFrame()
    lastWeek = set_last_week()
    thisWeek = set_this_week()
    for week in range((max_week+1),thisWeek):
        #Setting this sleep timer on a few weeks helps with the rapid requests to the Yahoo servers
        #If you request the site too much in a short amount of time you will be blocked temporarily  
        for matchup in range(1,(num_teams+1)):
            soup = url_requests(YAHOO_LEAGUE_ID+'matchup?week='+str(week)+'&module=matchup&mid1='+str(matchup))
            table = soup.find_all('table')
            df = pd.read_html(str(table))[1]
            df.columns = df.columns.str.replace('[#,@,&,/,+]', '')
            df['Week']=week
            df=df[['Team','Week','Score']]
            df['Opponent'] = df.loc[1,'Team']
            df['Opponent_Score'] = df.loc[1,'Score']
            
            
            # This is the best way to calculate success rate. Ties will factor out as a 0-0-12 week generates the same score as a 6-6 week, or a 5-5-2 week,4-4-4 week, etc. (6)
            # You can also calculate total score/12 I guess... But we've come way to far for that
            df['Score_Difference'] = df['Score'] - df['Opponent_Score']
            # Calculate the minimum and maximum values of the 'Score_Difference' column
            min_value = -1*(num_teams)
            max_value = num_teams

            # Normalize the 'Score_Difference' column to a range of 0 to 1
            df['Normalized_Score_Difference'] = (df['Score_Difference'] - min_value) / (max_value - min_value)

            weekly_results_df = weekly_results_df.append(df.loc[0], True)
            print(weekly_results_df)


    weekly_results_df = build_team_numbers(weekly_results_df)

    return weekly_results_df 

def get_weekly_stats(num_teams,leaguedf,most_recent_week):
    thisWeek = set_this_week()
    running_df=pd.DataFrame()
    for week in range ((most_recent_week+1),thisWeek):
        #Function below sets up the dataframe for the all play function
        if most_recent_week+1 == thisWeek:
            pass
        else:
            allPlaydf = leaguedf
            for matchup in range(1, (num_teams+1)):
                # Setting this sleep timer on a few weeks helps with the rapid requests to the Yahoo servers
                # If you request the site too much in a short amount of time, you will be blocked temporarily          

                soup = url_requests(YAHOO_LEAGUE_ID + 'matchup?week=' + str(week) + '&module=matchup&mid1=' + str(matchup))

                table = soup.find_all('table')
                df = pd.read_html(str(table))[1]
                df['Week'] = week
                print(df)
                df.columns = df.columns.str.replace('[#,@,&,/,+]', '')
                df.columns = df.columns.str.replace('HR.1', 'HRA')

                for column in df.columns:
                    if column in percentage_categories:

                        # Logic below to handle asterisks that happen for % based stats when ties occur
                        df[column] = df[column].astype(str)  # Convert column to string type
                
                        # Remove asterisks from column values
                        df[column] = df[column].map(lambda x: x.rstrip('*'))
                        
                        # Replace '-' with '0.00'
                        df[column] = df[column].replace(['-'], '0.00')
                        
                        df[column] = df[column].astype(float)  # Convert column to float type

                column_list = leaguedf.columns.tolist()
                df = df[column_list]
                df['Opponent'] = df.loc[1, 'Team']

                allPlaydf = allPlaydf.append(df.loc[0], True)
            
            logger.info(f'Week: {week}')
            running_df = running_df.append(allPlaydf)
            print(running_df)
    return running_df

def get_running_stats(df):
    df = df.drop('_id', axis=1)
    # Exclude 'Team', 'Week', 'Opponent' columns
    cols_to_sum = [col for col in df.columns if col not in ['Team', 'Week', 'Opponent']]

       
    # Create a new DataFrame to store the running totals or averages
    totals_df = pd.DataFrame(columns=df.columns)

    # Group the DataFrame by 'Team' column
    grouped = df.groupby('Team')

    # Initialize a dictionary to store the running totals or averages for each team
    team_totals = {}

    # Iterate through each group (team) and calculate running totals or averages
    for team, group in grouped:
        # Initialize the running totals or averages dictionary for the current team
        team_totals[team] = {}

        # Initialize the running totals for each column
        running_totals = {col: 0 for col in cols_to_sum}

        # Iterate through each row in the group (team)
        for _, row in group.iterrows():
            # Update the running totals for each column
            for col in cols_to_sum:
                # Calculate running averages for percentage categories excluding week 1
                if col in percentage_categories:
                    if row['Week'] == 1:
                        running_totals[col] = row[col]
                    else:
                        running_totals[col] = (running_totals[col] * (row['Week'] - 1) + row[col]) / (row['Week'])
                else:
                    print(row)
                    # Calculate running totals for other categories
                    running_totals[col] += row[col]

            # Update the running totals in the team_totals dictionary
            team_totals[team][row['Week']] = running_totals.copy()

    # Iterate through the original DataFrame and populate the totals_df with the running totals or averages
    for _, row in df.iterrows():
        team = row['Team']
        week = row['Week']
        running_totals = team_totals[team][week]

        # Append the row with the running totals or averages to the totals_df
        totals_df = totals_df.append({**row, **running_totals}, ignore_index=True)

    # Sort the totals_df by 'Week' and 'Team' columns
    totals_df = totals_df.sort_values(['Week', 'Team'])

        # Iterate through each week and rank teams in each category
    for week in range(1, totals_df['Week'].max() + 1):
        week_mask = totals_df['Week'] <= week
        week_df = totals_df[week_mask]

        # Iterate through each category
        for col in cols_to_sum:
            rank_col = col + '_Rank_Stats'
            if col in percentage_categories:
                if col in Low_Categories:
                    # Calculate the ranks for the current week and category
                    ranks = week_df.groupby('Week')[col].rank(ascending=True)
                
                else:
                    # Calculate the ranks for the current week and category
                    ranks = week_df.groupby('Week')[col].rank(ascending=False)
            else:
                if col in Low_Categories:
                    # Calculate the ranks for the current week and category
                    ranks = week_df.groupby('Week')[col].rank(ascending=True)
                
                else:
                    # Calculate the ranks for the current week and category
                    ranks = week_df.groupby('Week')[col].rank(ascending=False)
                
            # Assign the ranks to the corresponding week and category in the totals_df
            totals_df.loc[week_mask, rank_col] = ranks


    # Get the columns with '_Rank_Stats'
    rank_stats_cols = [col for col in totals_df.columns if '_Rank_Stats' in col]

    # Calculate the average for each week and team
    averages = totals_df.groupby(['Week', 'Team'])[rank_stats_cols].mean().reset_index()

    # Create the 'Stats_Power_Rank' column and calculate the average of rank stats for each week
    averages['Stats_Power_Rank'] = averages[rank_stats_cols].mean(axis=1)

    # Merge the averages with totals_df
    totals_df = totals_df.merge(averages[['Week', 'Team', 'Stats_Power_Rank']], on=['Week', 'Team'])

    # Sort the totals_df by 'Week' and 'Team' columns
    totals_df = totals_df.sort_values(['Week', 'Team']) 

    # Print the totals_df
    print(totals_df)
    return(totals_df)

def main():
    num_teams = league_size()
    leaguedf = league_stats_all_df()
    try:
        #Aggregate W/L thorughout season
        clear_mongo(MONGO_DB,'weekly_results')
        df = get_mongo_data(MONGO_DB,'weekly_results','')
        print(df)
        if not df.empty: 
            max_week = df['Week'].max()
            weekly_results_df = get_weekly_results(num_teams,max_week)
            if weekly_results_df is not None and not weekly_results_df.empty:
                print(weekly_results_df)
                write_mongo(MONGO_DB,weekly_results_df,'weekly_results')
            else:
                pass
        else:
            weekly_results_df = get_weekly_results(num_teams,0)
            if weekly_results_df is not None:
                write_mongo(MONGO_DB,weekly_results_df,'weekly_results')

        # Aggregate Stats
        rank_df = get_mongo_data(MONGO_DB,'weekly_stats','')
        if not rank_df.empty: 
            max_week = rank_df['Week'].max()
            weekly_stats_df = get_weekly_stats(num_teams,leaguedf,max_week)
            if weekly_stats_df is not None and not weekly_stats_df.empty:
                print(weekly_stats_df)
                write_mongo(MONGO_DB,weekly_stats_df,'weekly_stats')
            else:
                pass
        
        else:
            weekly_stats_df = get_weekly_stats(num_teams,leaguedf,0)
            if weekly_stats_df is not None:
                write_mongo(MONGO_DB,weekly_stats_df,'weekly_stats')

        #Generate ranks and running ranks in lieu of running power ranks which started at the beginning of the season
        weekly_stats_df = get_mongo_data(MONGO_DB,'weekly_stats','')
        run_stats_df = get_running_stats(weekly_stats_df)
        clear_mongo(MONGO_DB,'power_ranks_lite')
        write_mongo(MONGO_DB,run_stats_df,'power_ranks_lite')

    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        send_failure_email(error_message, filename)

if __name__ == '__main__':
    main()
