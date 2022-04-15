from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
import pandas as pd
import requests
from bs4 import BeautifulSoup
import bs4 as bs
import urllib
import csv
import urllib.request
from functools import reduce
import json
from pymongo import MongoClient, collection
import time, datetime
import certifi

my_date = datetime.date.today()
year, week_num, day_of_week = my_date.isocalendar()

def getAllplay():
    #Set week number
    my_date = datetime.date.today()
    year, week_num, day_of_week = my_date.isocalendar()
    thisWeek = (week_num - 14)
    # Loop through weeks and matchups
    sleep_nums = [2,5,8,11,14,17]
    for week in range(1,thisWeek):
        allPlaydf = pd.DataFrame(columns = ['Team','Week','R','H','HR','SB','OPS','RBI','ERA','WHIP','K9','QS','SVH','NW'])
        for matchup in range(1,13):
            data=[]      
            #Setting this sleep timer on a few weeks helps with the rapid requests to the Yahoo servers
            #If you request the site too much in a short amount of time you will be blocked temporarily
            if matchup in sleep_nums:
                time.sleep(6)     
            else:
                pass
            source = urllib.request.urlopen('https://baseball.fantasysports.yahoo.com/b1/11602/matchup?week='+str(week)+'&module=matchup&mid1='+str(matchup)).read()
            soup = bs.BeautifulSoup(source,'lxml')

            table = soup.find_all('table')
            df = pd.read_html(str(table))[1]
            df['Week'] = week

            df.columns = df.columns.str.replace('[#,@,&,/,+]', '')
            
            # Logic below to handle asterisks that happen for % based stats when ties occue
            df['WHIP'] = df['WHIP'].astype(str)
            df['OPS'] = df['OPS'].astype(str)
            df['K9'] = df['K9'].astype(str)
            

            df['WHIP'] = df['WHIP'].map(lambda x: x.rstrip('*'))
            df['OPS'] = df['OPS'].map(lambda x: x.rstrip('*'))
            df['K9'] = df['K9'].map(lambda x: x.rstrip('*'))

            df['WHIP'] = df['WHIP'].astype(float)
            df['OPS'] = df['OPS'].astype(float)
            df['K9'] = df['K9'].astype(float)


            df=df[['Team','Week','R','H','HR','SB','OPS','RBI','ERA','WHIP','K9','QS','SVH','NW']]

            allPlaydf = allPlaydf.append(df.loc[0], True)
    
        print(week)
    
        # Calculate implied win statistics - The person with the most Runs in a week has an impied win of 1.0, because they would defeat every other team in that category.
        # Lowest scoring player has implied wins of 0, which we manually set to avoid dividing by 0

        for column in allPlaydf:
            if column == 'Team' or column == 'Week':
                pass
            elif column == 'ERA' or column == 'WHIP':
                allPlaydf[column+'_Rank'] = allPlaydf[column].rank(ascending = False)-1
                # Set the index to newly created column, Rating_Rank
                allPlaydf.set_index(column+'_Rank')
                allPlaydf[column+'_Coeff'] = allPlaydf[column+'_Rank']/11
            else:
                allPlaydf[column+'_Rank'] = allPlaydf[column].rank(ascending = True)-1
                # Set the index to newly created column, Rating_Rank
                allPlaydf.set_index(column+'_Rank')
                allPlaydf[column+'_Coeff'] = allPlaydf[column+'_Rank']/11

            coeff_cols = [col for col in allPlaydf.columns if 'Coeff' in col]
            coeff_cols.append('Team')
            coeff_cols.append('Week')
            rankings_df = allPlaydf[coeff_cols]
        
        cols_to_sum = rankings_df.columns[ : df.shape[1]-1]
        rankings_df['Expected_Wins'] = rankings_df[cols_to_sum].sum(axis=1)

        # Set Up Connections
        ca = certifi.where()
        client = MongoClient("mongodb+srv://admin:Aggies_1435@cluster0.qj2j8.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&verify=false", tlsCAFile=ca)
        db = client['YahooFantasyBaseball']
        collection = db['coefficient']
        
        #Delete Existing Documents
        myquery = {"Week":week}
        x = collection.delete_many(myquery)
        print(x.deleted_count, " documents deleted.")

        # Reset Index and insert entire DF into MondgoDB
        # df.reset_index(inplace=True)
        data_dict = rankings_df.to_dict("records")
        collection.insert_many(data_dict)
        client.close()
        
        # Reset dfs for new weeks so data isn't aggregated
        del allPlaydf,rankings_df


def toMongo(df):
    
    # Set Up Connections
    ca = certifi.where()
    client = MongoClient("mongodb+srv://admin:Aggies_1435@cluster0.qj2j8.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&verify=false", tlsCAFile=ca)
    db = client['YahooFantasyBaseball']
    collection = db['coefficient']
    
    # Reset Index and insert entire DF into MondgoDB
    df.reset_index(inplace=True)
    data_dict = df.to_dict("records")
    collection.insert_many(data_dict)
    client.close()


    # db.collection.insert(records)



def main():
    rankings_df = getAllplay()
    toMongo(rankings_df)

if __name__ == '__main__':
    main()