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
    def getLiveStandings():
        #Set week number
        my_date = datetime.date.today()
        year, week_num, day_of_week = my_date.isocalendar()
        thisWeek = (week_num - 15)

        source = uReq('https://baseball.fantasysports.yahoo.com/b1/11602').read()
        soup = bs.BeautifulSoup(source,'lxml')

        table = soup.find_all('table')
        df_seasonRecords = pd.read_html(str(table))[0]

        df_seasonRecords.columns = df_seasonRecords.columns.str.replace('[-]', '')

        new = df_seasonRecords['WLT'].str.split("-", n = 2, expand = True)
        new = new.astype(int)

        # making separate first name column from new data frame
        df_seasonRecords["Week"]= thisWeek
        df_seasonRecords["WLT_Win"]= new[0]
        df_seasonRecords["WLT_Loss"]= new[1]
        df_seasonRecords["WLT_Draw"]= new[2]
        df_seasonRecords["Raw_Score"]= df_seasonRecords["WLT_Win"] + df_seasonRecords["WLT_Draw"]*.5
        df_seasonRecords['Games_Back'] = df_seasonRecords['Raw_Score'].max() - df_seasonRecords['Raw_Score']
        df_seasonRecords['Pct'] = (df_seasonRecords['WLT_Win'] + (df_seasonRecords['WLT_Draw']*.5))/(df_seasonRecords['WLT_Win'] + df_seasonRecords['WLT_Draw'] + df_seasonRecords['WLT_Loss'])

        #season standings numbers (Must adjust for draws counting 0.5)
        df_seasonRecords = df_seasonRecords.sort_values(by=['Pct'],ascending=False,ignore_index=True)
        print(df_seasonRecords[['Team','Pct','Raw_Score','Week']].sort_values(by=['Pct'],ascending=False,ignore_index=True))

    def mongo_write(df_liveStandings):
        #Connect to Mongo, the ca is for ignoring TLS/SSL handshake issues
        ca = certifi.where()
        client = pymongo.MongoClient("mongodb+srv://admin:Aggies_1435@cluster0.qj2j8.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&verify=false", tlsCAFile=ca)
        db = client['YahooFantasyBaseball']
        collection = db['season_standings']

        #Delete Existing Documents
        #myquery = {}
        #x = collection.delete_many(myquery)
        #print(x.deleted_count, " documents deleted.")


        #Insert New Live Standings
        df_liveStandings.reset_index(inplace=True)
        data_dict = df_liveStandings.to_dict("records")
        collection.insert_many(data_dict)
        client.close()
        


except Exception as e:
    print(str(e))

def main():
    df_liveStandings = getLiveStandings()
    #mongo_write(df_liveStandings)


if __name__ == '__main__':
    main()
