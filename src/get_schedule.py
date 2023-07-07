import pandas as pd
import bs4 as bs
import urllib.request
from urllib.request import urlopen as uReq
from pymongo import MongoClient
import time, datetime, os, sys
from dotenv import load_dotenv

# Local Modules - email utils for failure emails, mongo utils to 
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

def get_schedule():
    num_teams = league_size()
    schedule_df = pd.DataFrame(columns = ['Team','Opponent','Week'])
    this_week = set_this_week()
    for week in range(1,24):
        for matchup in range(1,(num_teams+1)):
            soup = url_requests(YAHOO_LEAGUE_ID+'matchup?date=totals&week='+str(week)+'&mid1='+str(matchup))
            table = soup.find_all('table')
            df = pd.read_html(str(table))[1]
            df['Week'] = week
            df.columns = df.columns.str.replace('[#,@,&,/,+]', '')

            df['Opponent'] = df.loc[1,'Team']
            df=df[['Team','Opponent','Week']]

            schedule_df = schedule_df.append(df.loc[0], True)
            print(schedule_df)



        #BUILD TABLE WITH TEAM NAME AND NUMBER
        source = uReq(YAHOO_LEAGUE_ID).read()
        soup = bs.BeautifulSoup(source,'lxml')

        table = soup.find('table')  # Use find() to get the first table

        # Extract all href links from the table, if found
        if table is not None:
            links = []
            for link in table.find_all('a'):  # Find all <a> tags within the table
                link_text = link.text.strip()  # Extract the hyperlink text
                link_url = link['href']  # Extract the href link
                if link_text != '':
                    links.append((link_text, link_url))  # Append the hyperlink text and link to the list


        # Map team numbers from the dictionary to a new Series
        #Iterate through the rows of the DataFrame
        for index, row in schedule_df.iterrows():
            team_name = row['Team']
            for link in links:
                if link[0] == team_name:
                    team_number = link[1][-2:] if link[1][-2:].isdigit() else link[1][-1:] # Grab the last 2 characters if they are both digits, else grab the last character
                    schedule_df.at[index, 'Team_Number'] = team_number
                    break
        
        #Opponent Team Numbers
        for index, row in schedule_df.iterrows():
            team_name = row['Opponent']
            for link in links:
                if link[0] == team_name:
                    team_number = link[1][-2:] if link[1][-2:].isdigit() else link[1][-1:] # Grab the last 2 characters if they are both digits, else grab the last character
                    schedule_df.at[index, 'Opponent_Team_Number'] = team_number
                    break
        
        schedule_df = schedule_df.drop(['Team', 'Opponent'], axis=1)
        print(schedule_df)


        write_mongo(MONGO_DB,schedule_df,'schedule')

        schedule_df = pd.DataFrame(columns = ['Team','Opponent','Week'])


def main():
    try:
        get_schedule()
    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        send_failure_email(error_message, filename)

if __name__ == '__main__':
    main()
