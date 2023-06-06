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
from mongo_utils import mongo_write_team_IDs
from manager_dict import manager_dict
from datetime_utils import set_this_week

# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')


def getCurrentMatchups():
    
    df_teamRecords = pd.DataFrame(columns = ['Team','Team_Wins','Team_Loss','Team_Draw','Record'])
    df_weeklyMatchups = pd.DataFrame(columns = ['Team','Record'])

    # Change this to the number of teams in your league (I have 12)
    for matchup in range(1,13):
        #To prevent DDOS, Yahoo limits your URL requests over a set amount of time. Sleep timer to hlep space our requests
        time.sleep(4)
        df_currentMatchup = pd.DataFrame(columns = ['Team','Score'])
        
        thisWeek = set_this_week()

        #This is the correct URL, it gets team totals as opposed to the standard matchup page which has weird embedded tables
        source = uReq(YAHOO_LEAGUE_ID+'matchup?date=totals&week='+str(thisWeek)+'&mid1='+str(matchup)).read()
        print(YAHOO_LEAGUE_ID+'matchup?date=totals&week='+str(thisWeek)+'&mid1='+str(matchup))
        soup = bs.BeautifulSoup(source,'lxml')

        table = soup.find_all('table')
        df_currentMatchup = pd.read_html(str(table))[1]

        print(df_currentMatchup)
        
        df_currentMatchup=df_currentMatchup[['Team','Score']]

        df_teamRecords['Team'] = df_currentMatchup['Team']
        df_teamRecords['Team_Wins'] = df_currentMatchup['Score'].iloc[0]
        df_teamRecords['Team_Loss'] = df_currentMatchup['Score'].iloc[1]
        if df_teamRecords['Team_Wins'].iloc[0] + df_teamRecords['Team_Loss'].iloc[0] == 12:
            df_teamRecords['Team_Draw'] = 0
            df_teamRecords['Record'] = list(zip(df_teamRecords.Team_Wins,df_teamRecords.Team_Draw,df_teamRecords.Team_Loss))
        else:
            df_teamRecords['Team_Draw'] = 12 - (df_teamRecords['Team_Wins'].iloc[0] + df_teamRecords['Team_Loss'].iloc[0])
            df_teamRecords['Record'] = list(zip(df_teamRecords.Team_Wins,df_teamRecords.Team_Draw,df_teamRecords.Team_Loss))
        
        # print(df_teamRecords[['Team','Record']].loc[0])

        df_weeklyMatchups = df_weeklyMatchups.append(df_teamRecords.loc[0], True)

    #print(df_weeklyMatchups[['Team','Record']])
    
    return df_weeklyMatchups



def getLiveStandings(df_currentMatchup):
    source = uReq(YAHOO_LEAGUE_ID).read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    df_seasonRecords = pd.read_html(str(table))[0]

    df_seasonRecords.columns = df_seasonRecords.columns.str.replace('[-]', '')

    new = df_seasonRecords['WLT'].str.split("-", n = 2, expand = True)
    new = new.astype(int)
    # making separate first name column from new data frame
    df_seasonRecords["WLT_Win"]= new[0]
    df_seasonRecords["WLT_Loss"]= new[1]
    df_seasonRecords["WLT_Draw"]= new[2]
    df_seasonRecords['Raw_Score_Static'] = (df_seasonRecords['WLT_Win'] + (df_seasonRecords['WLT_Draw']*.5))

    #calculate live standings from season records and live records
    df_liveStandings = df_seasonRecords.merge(df_currentMatchup, on='Team')
    df_liveStandings['Live_Wins'] = df_liveStandings['WLT_Win'] + df_liveStandings['Team_Wins']
    df_liveStandings['Live_Loss'] = df_liveStandings['WLT_Loss'] + df_liveStandings['Team_Loss']
    df_liveStandings['Live_Draw'] = df_liveStandings['WLT_Draw'] + df_liveStandings['Team_Draw']
    df_liveStandings['Raw_Score'] = (df_liveStandings['Live_Wins'] + (df_liveStandings['Live_Draw']*.5))
    df_liveStandings['Games_Back'] = df_liveStandings['Raw_Score'].max() - df_liveStandings['Raw_Score']
    df_liveStandings['Pct'] = (df_liveStandings['Live_Wins'] + (df_liveStandings['Live_Draw']*.5))/(df_liveStandings['Live_Wins'] + df_liveStandings['Live_Draw'] + df_liveStandings['Live_Loss'])
    df_liveStandings['Current Matchup'] = df_liveStandings['Team_Wins'].astype(int).astype(str) + '-' + df_liveStandings['Team_Loss'].astype(int).astype(str) + '-' + df_liveStandings['Team_Draw'].astype(int).astype(str)
    df_liveStandings = df_liveStandings.sort_values(by=['Pct'],ascending=False,ignore_index=True)
    print(df_liveStandings[['Team','Pct','Raw_Score']].sort_values(by=['Pct'],ascending=False,ignore_index=True))
    #Need to change below when people clinch playoffs
    try:        
        df_liveStandings['Rank'] = df_liveStandings['Rank'].str.replace('*','').astype(int)
    except AttributeError:
        print("No one has clinched playoffs yet, yo")


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

        # Print the extracted links and their associated hyperlink text
        for link_text, link_url in links:
            print(f'{link_text}, {link_url[-1]}')
        
        #Here contains the Team name and team number
        result_dict = {link_url[-1]: link_text for link_text, link_url in links if link_text != ''}
        print(result_dict)

    #Map team numbers from the dictionary to a new Series
    #Iterate through the rows of the DataFrame
    for index, row in df_liveStandings.iterrows():
        team_name = row['Team']
        for link in links:
            if link[0] == team_name:
                team_number = link[1][-2:] if link[1][-2:].isdigit() else link[1][-1:] # Grab the last 2 characters if they are both digits, else grab the last character
                df_liveStandings.at[index, 'Team_Number'] = team_number
                break
    df_liveStandings['Manager_Name'] = df_liveStandings['Team_Number'].map(manager_dict)
    

    return df_liveStandings

def mongo_write(df_liveStandings):
    #Connect to Mongo, the ca is for ignoring TLS/SSL handshake issues
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)
    db = client['YahooFantasyBaseball_2023']
    collection = db['live_standings']

    #Delete Existing Documents
    myquery = {}
    x = collection.delete_many(myquery)
    print(x.deleted_count, " documents deleted.")


    #Insert New Live Standings
    df_liveStandings.reset_index(inplace=True)
    data_dict = df_liveStandings.to_dict("records")
    collection.insert_many(data_dict)
    client.close()
    

def main():
    try:
        df_currentMatchup = getCurrentMatchups()
        df_liveStandings = getLiveStandings(df_currentMatchup)
        mongo_write(df_liveStandings)
        mongo_write_team_IDs(df_liveStandings)
    except Exception as e:
        filename = os.path.basename(__file__)
        error_message = str(e)
        send_failure_email(error_message,filename)


if __name__ == '__main__':
    main()
