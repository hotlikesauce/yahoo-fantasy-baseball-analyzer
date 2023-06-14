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
from datetime_utils import *
from manager_dict import manager_dict

# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')

def get_schedule():
    schedule_df = pd.DataFrame(columns = ['Team','Opponent','Week'])
    this_week = set_this_week()
    for week in range(1,22):
        for matchup in range(1,13):
            time.sleep(3)

        
            try:
                source = urllib.request.urlopen(YAHOO_LEAGUE_ID+'matchup?date=totals&week='+str(week)+'&mid1='+str(matchup)).read()
                soup = bs.BeautifulSoup(source,'lxml')
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    print("Error 404: Page not found. Retrying...")
                    print(YAHOO_LEAGUE_ID+'matchup?pspid=782201763&activity=matchups&week='+str(week))
                    source = urllib.request.urlopen(YAHOO_LEAGUE_ID+'matchup?date=totals&week='+str(week)+'&mid1='+str(matchup)).read()
                else:
                    raise e


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


        # Set Up Connections
        ca = certifi.where()
        client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)
        
        db = client['YahooFantasyBaseball_2023']
        collection = db['schedule']

        # #Delete Existing Documents From Last Week Only
        # myquery = {"Week":week}
        # x = collection.delete_many(myquery)
        # print(x.deleted_count, " documents deleted.")

        
        # Reset Index and insert entire DF into MondgoDB
        df.reset_index(inplace=True)
        data_dict = schedule_df.to_dict("records")
        collection.insert_many(data_dict)
        client.close()

        schedule_df = pd.DataFrame(columns = ['Team','Opponent','Week'])


def main():
    try:
        get_schedule()
    except Exception as e:
        filename = os.path.basename(__file__)
        error_message = str(e)
        send_failure_email(error_message,filename)

if __name__ == '__main__':
    main()
