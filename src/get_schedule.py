import pandas as pd
import bs4 as bs
import urllib.request
from urllib.request import urlopen as uReq
from pymongo import MongoClient
import time, datetime, os, sys
import numpy as np
from dotenv import load_dotenv
import warnings
# Ignore the FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

# Local Modules
from email_utils import send_failure_email
from datetime_utils import *
from mongo_utils import *
from manager_dict import manager_dict
from yahoo_utils import *

# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')
MONGO_DB = os.environ.get('MONGO_DB')

def get_schedule(max_week):
    num_teams = league_size()
    this_week = set_this_week()

    for week in range(1, 22):
        rows = []
        for matchup in range(1, num_teams + 1):
            soup = url_requests(YAHOO_LEAGUE_ID + f'matchup?date=totals&week={week}&mid1={matchup}')
            table = soup.find_all('table')
            df = pd.read_html(str(table))[1]
            df['Week'] = week
            df.columns = df.columns.str.replace('[#,@,&,/,+]', '', regex=True)

            df['Opponent'] = df.loc[1, 'Team']
            df = df[['Team', 'Opponent', 'Week']]
            rows.append(df.loc[0])

        # Build the full schedule DataFrame
        schedule_df = pd.DataFrame(rows)
        print(schedule_df)

        # Check for missing opponent info
        if schedule_df['Opponent'].isnull().any():
            return

        # BUILD TABLE WITH TEAM NAME AND NUMBER
        source = uReq(YAHOO_LEAGUE_ID).read()
        soup = bs.BeautifulSoup(source, 'lxml')

        table = soup.find('table')
        links = []
        if table is not None:
            for link in table.find_all('a'):
                link_text = link.text.strip()
                link_url = link['href']
                if link_text:
                    links.append((link_text, link_url))

        print(schedule_df)
        # Map team numbers
        # Map team numbers
        for index, row in schedule_df.iterrows():
            team_name = row['Team']
            team_number = None  # Initialize team_number to avoid overwriting in each iteration
            
            # Loop through each link and find the correct team match
            for link in links:
                if link[0] == team_name:
                    team_number = link[1][-2:] if link[1][-2:].isdigit() else link[1][-1:]
                    break  # Exit the loop once a match is found
            
            # Only update the team number if it was found
            if team_number:
                schedule_df.at[index, 'Team_Number'] = team_number
                print(schedule_df)
                break  # Exit the loop once a match is found

        for index, row in schedule_df.iterrows():
            team_name = row['Opponent']
            for link in links:
                if link[0] == team_name:
                    team_number = link[1][-2:] if link[1][-2:].isdigit() else link[1][-1:]
                    schedule_df.at[index, 'Opponent_Team_Number'] = team_number
                    break

        schedule_df = schedule_df.drop(['Team', 'Opponent'], axis=1)
        print(schedule_df)

        write_mongo(MONGO_DB, schedule_df, 'schedule')

def main():
    try:
        df = get_mongo_data(MONGO_DB, 'schedule', '')
        if not df.empty:
            max_week = df['Week'].max() + 1
            get_schedule(max_week)
        else:
            get_schedule(1)
    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        print(error_message)
        send_failure_email(error_message, filename)

if __name__ == '__main__':
    main()
