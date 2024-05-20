import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
from pymongo import MongoClient
import time, datetime, os, sys
from dotenv import load_dotenv
from loguru import logger
import warnings
# Ignore the FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

# Local Modules - email utils for failure emails, mongo utils to 
from email_utils import send_failure_email
from datetime_utils import *
from manager_dict import manager_dict
from mongo_utils import *
from yahoo_utils import *

# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')
MONGO_DB = os.environ.get('MONGO_DB')


def get_all_play(num_teams,leaguedf,most_recent_week):
    thisWeek = set_this_week()
    for week in range ((most_recent_week),thisWeek):
        #Function below sets up the dataframe for the all play function
        if most_recent_week == thisWeek:
            pass
        elif most_recent_week == 0:
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

            print(week)
            logger.info(f'Week: {week}')

            # Calculate implied win statistics - The person with the most Runs in a week has an implied win of 1.0, because they would defeat every other team in that category.
            # Lowest scoring player has implied wins of 0, which we manually set to avoid dividing by 0
            for column in allPlaydf:
                if column in ['Team','Week','Opponent']:
                    pass
                elif column in Low_Categories:
                    allPlaydf[column+'_Rank'] = allPlaydf[column].rank(ascending=False) - 1
                    # Set the index to newly created column, Rating_Rank
                    allPlaydf.set_index(column+'_Rank')
                    allPlaydf[column+'_Coeff'] = allPlaydf[column+'_Rank'] / (num_teams - 1)
                else:
                    allPlaydf[column+'_Rank'] = allPlaydf[column].rank(ascending=True) - 1
                    # Set the index to newly created column, Rating_Rank
                    allPlaydf.set_index(column+'_Rank')
                    allPlaydf[column+'_Coeff'] = allPlaydf[column+'_Rank'] / (num_teams - 1)

                coeff_cols = [col for col in allPlaydf.columns if 'Coeff' in col]
                coeff_cols.append('Team')
                coeff_cols.append('Week')
                coeff_cols.append('Opponent')
                rankings_df = allPlaydf[coeff_cols]
            
            cols_to_sum = rankings_df.columns[:df.shape[1]-1]
            rankings_df['Expected_Wins'] = rankings_df[cols_to_sum].sum(axis=1)
            
            # Remove Individual Stat Columns
            rankings_df = rankings_df[['Team', 'Week', 'Opponent', 'Expected_Wins']]
            
            rankings_df_expanded = rankings_df.merge(right=rankings_df, left_on='Team', right_on='Opponent')
            
            rankings_df_expanded = rankings_df_expanded.rename(columns={"Team_x": "Team", "Week_x": "Week","Opponent_x": "Opponent","Expected_Wins_x": "Team_Expected_Wins","Expected_Wins_y": "Opponent_Expected_Wins"})
            rankings_df_expanded = rankings_df_expanded[['Week', 'Team', 'Team_Expected_Wins', 'Opponent', 'Opponent_Expected_Wins']]
            rankings_df_expanded['Matchup_Difference'] = (rankings_df_expanded['Team_Expected_Wins'] - rankings_df_expanded['Opponent_Expected_Wins']).apply(lambda x: round(x, 2))
            rankings_df_expanded['Matchup_Power'] = (rankings_df_expanded['Team_Expected_Wins'] + rankings_df_expanded['Opponent_Expected_Wins']).apply(lambda x: round(x, 2))
            
            print(rankings_df_expanded)
            
            df = build_team_numbers(rankings_df_expanded)
            df = build_opponent_numbers(rankings_df_expanded)

            
            #db name, collection, dataframe
            write_mongo(MONGO_DB,df,'coefficient')

            # Reset dfs for new weeks so data isn't aggregated
            del allPlaydf, rankings_df, df


def main():
    thisWeek = set_this_week()
    num_teams = league_size()
    leaguedf = league_stats_all_df()
    logger.add("logs/get_all_play.log", rotation="500 MB")
    for x in range(1,thisWeek):
        clear_mongo_query(MONGO_DB,'coefficient','"Week":'+str(x))
    try:
        df = get_mongo_data(MONGO_DB,'coefficient','')
        if not df.empty: 
            max_week = df['Week'].max()
            rankings_df = get_all_play(num_teams,leaguedf,max_week)
            if rankings_df is not None:
                write_mongo(MONGO_DB,rankings_df,'coefficient')
        else:
            rankings_df = get_all_play(num_teams,leaguedf,1)
            if rankings_df is not None:
                write_mongo(MONGO_DB,rankings_df,'coefficient')

    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        send_failure_email(error_message, filename)


if __name__ == '__main__':
    main()
