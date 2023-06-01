import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
import certifi
from pymongo import MongoClient
from datetime import datetime
import datetime, os
from dotenv import load_dotenv

# Local Modules - email utils for failure emails, mongo utils to 
from email_utils import send_failure_email
from datetime_utils import set_last_week
from player_dict import player_dict

# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')


def getStandings():

    #Set week number - These weeks are going to be 1 off since we are technically recording last week's data
    #Set week number
    lastWeek = set_last_week()

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

        # Print the extracted links and their associated hyperlink text
        # for link_text, link_url in links:
            # print(f'{link_text}, {link_url[-1]}')
        
        #Here contains the Team name and team number
        result_dict = {link_url[-1]: link_text for link_text, link_url in links if link_text != ''}
        print(result_dict)


    df_seasonRecords = pd.read_html(str(table))[0]
    
    df_seasonRecords.columns = df_seasonRecords.columns.str.replace('[-]', '')

    new = df_seasonRecords['WLT'].str.split("-", n = 2, expand = True)
    new = new.astype(int)

    # making separate first name column from new data frame
    df_seasonRecords["Week"]= lastWeek
    df_seasonRecords["WLT_Win"]= new[0]
    df_seasonRecords["WLT_Loss"]= new[1]
    df_seasonRecords["WLT_Draw"]= new[2]
    df_seasonRecords["Raw_Score"]= df_seasonRecords["WLT_Win"] + df_seasonRecords["WLT_Draw"]*.5
    df_seasonRecords['Games_Back'] = df_seasonRecords['Raw_Score'].max() - df_seasonRecords['Raw_Score']
    df_seasonRecords['Pct'] = (df_seasonRecords['WLT_Win'] + (df_seasonRecords['WLT_Draw']*.5))/(df_seasonRecords['WLT_Win'] + df_seasonRecords['WLT_Draw'] + df_seasonRecords['WLT_Loss'])

    # Season standings numbers (Must adjust for draws counting 0.5)
    df_seasonRecords = df_seasonRecords.sort_values(by=['Pct'],ascending=False,ignore_index=True)

    # Map team numbers from the dictionary to a new Series
    # Iterate through the rows of the DataFrame
    for index, row in df_seasonRecords.iterrows():
        team_name = row['Team']
        for link in links:
            if link[0] == team_name:
                team_number = link[1][-2:] if link[1][-2:].isdigit() else link[1][-1:] # Grab the last 2 characters if they are both digits, else grab the last character
                df_seasonRecords.at[index, 'Team_Number'] = team_number
                break
    df_seasonRecords['Player_Name'] = df_seasonRecords['Team_Number'].map(player_dict)
    print(df_seasonRecords.sort_values(by=['Pct'],ascending=False,ignore_index=True))

    return df_seasonRecords

def mongo_write(df_Standings):
    #Connect to Mongo, the ca is for ignoring TLS/SSL handshake issues
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)

    db = client['YahooFantasyBaseball_2023']
    collection = db['standings_season_trend']

    #Insert New Season Standings
    df_Standings.reset_index(inplace=True)
    data_dict = df_Standings.to_dict("records")
    collection.insert_many(data_dict)
    client.close()
    
def clearMongo():
    lastWeek = set_last_week()
    # Set Up Connections
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)
    db = client['YahooFantasyBaseball_2023']
    collection = db['standings_season_trend']

    #Delete Existing Documents From Last Week Only
    myquery = {"Week":lastWeek}
    x = collection.delete_many(myquery)
    print(x.deleted_count, " documents deleted.")   


def main():
    try:
        clearMongo()
        df_Standings = getStandings()
        mongo_write(df_Standings)
    except Exception as e:
        filename = os.path.basename(__file__)
        error_message = str(e)
        send_failure_email(error_message,filename)


if __name__ == '__main__':
    main()
