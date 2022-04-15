import pandas as pd
import requests
from bs4 import BeautifulSoup
import bs4 as bs
import urllib
import csv
import urllib.request
from urllib.request import urlopen as uReq
from functools import reduce
from time import sleep
import numpy as np
import pymongo, certifi
from datetime import datetime
import datetime

try:
    def getCurrentMatchups():
        
        df_teamRecords = pd.DataFrame(columns = ['Team','Team_Wins','Team_Loss','Team_Draw','Record'])
        df_weeklyMatchups = pd.DataFrame(columns = ['Team','Record'])

        for matchup in range(1,13):
            df_currentMatchup = pd.DataFrame(columns = ['Team','Score'])
            
            #Set week number
            my_date = datetime.date.today()
            year, week_num, day_of_week = my_date.isocalendar()
            thisWeek = (week_num - 14)

            #This is the correct URL, it gets team totals as opposed to the standard matchup page which has weird embedded tables
            source = uReq('https://baseball.fantasysports.yahoo.com/b1/11602/matchup?date=totals&week='+str(thisWeek)+'&mid1='+str(matchup)).read()
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
        source = uReq('https://baseball.fantasysports.yahoo.com/b1/11602').read()
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

        return df_liveStandings

    def mongo_write(df_liveStandings):
        #Connect to Mongo, the ca is for ignoring TLS/SSL handshake issues
        ca = certifi.where()
        client = pymongo.MongoClient("mongodb+srv://admin:Aggies_1435@cluster0.qj2j8.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&verify=false", tlsCAFile=ca)
        db = client['YahooFantasyBaseball']
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
        


except Exception as e:
    print(str(e))

def main():
    df_currentMatchup = getCurrentMatchups()
    df_liveStandings = getLiveStandings(df_currentMatchup)
    mongo_write(df_liveStandings)


if __name__ == '__main__':
    main()
