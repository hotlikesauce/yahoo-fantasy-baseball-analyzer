from os import name
from scrapeStandings import write_mongo
import pandas as pd, urllib,unicodedata,time
import requests
from bs4 import BeautifulSoup
import bs4 as bs
import numpy as np
import pymongo

from pybaseball import playerid_lookup
from pybaseball import statcast_pitcher
from pybaseball import pitching_stats
from pybaseball import batting_stats_range


def setup_tables():
    df_players = pd.read_csv('PlayerIDMap.csv')
    
    df_players = df_players[["PLAYERNAME","FIRSTNAME","LASTNAME","POS","YAHOONAME","YAHOOID","MLBID","BREFID","ESPNID","IDFANGRAPHS"]]
    df_players = df_players[df_players['POS'] != 'P']






    #convert to csv and alert any NAs
    df_players.to_csv("Player_Dictionary.csv")
    df_players.to_csv("Batters_Dict.csv")
    

    
    print(df_players[df_players.isna().any(axis=1)])


def get_stats():
    df_main = pd.DataFrame(columns = ["ESPNID","GP","AB","R","H","2B","3B","HR","RBI","BB","HBP","SO","SB","CS","AVG","OBP","SLG","OPS","WAR"])
    batters_dict = pd.read_csv('Batters_Dict.csv')

    x=0
    for player in batters_dict.index:
        #Basically convert float to int        
        x=x+1
        print(x)
        
        #handle 404 error
        try:
            source = urllib.request.urlopen("https://www.espn.com/mlb/player/stats/_/id/"+(str(batters_dict['ESPNID'][player])[:-2])).read()
        
            soup = bs.BeautifulSoup(source,'lxml')

            table = soup.find_all('table')

            try:
                #determine if a player has played for > 1 team
                df = pd.read_html(str(table))[0]
                df_2021 = (df.loc[df['season'] == '2021'])
                
                if len(df_2021.index) == 1:
                    
                    #Try/Except to handle players who were drafted but yet to appear in an MLB game this year

                    df = pd.read_html(str(table))[1]
                    
                    #Most recent season stats
                    df = df.iloc[-3]

                    #Add variables name to df
                    df["Team"] = batters_dict['Team'][player]
                    df["Player"] = batters_dict['PLAYERNAME'][player]
                    df["ESPNID"] = batters_dict['ESPNID'][player]

                    df_main = df_main.append(df,ignore_index=True)

                else:
                    df = pd.read_html(str(table))[1]

                    #calculate number of teams played for, grab corresponding rows
                    seasons = (-2)-len(df_2021.index)
                    
                    #Sum Most recent season stats
                    df = (df.iloc[seasons:-3])
                    df = df.sum()

                    #Add player name to df
                    df["Team"] = batters_dict['Team'][player]
                    df["Player"] = batters_dict['PLAYERNAME'][player]
                    df["ESPNID"] = batters_dict['ESPNID'][player]

                    df_main = df_main.append(df,ignore_index=True)
            
            except ValueError:
                pass


        except Exception as e:
            print(e)
            continue
    
    
    df_main['OPS_Weighted'] = df_main['OPS'] * df_main['AB']
    time.sleep(60)
    df_main.to_csv('Current_Stats.csv')
    print(df_main)


def compile_stats():
    df_stats_dict = pd.read_csv("Current_Stats.csv")
    df_draft_dict = pd.read_csv("Batters_Dict.csv")
    print(df_stats_dict)


    for column in df_stats_dict:
        if column in ['ESPNID','AB','GP','2B','3B','BB','HBP','SO','CS','AVG','OBP','SLG']:
            pass
        else:
            df_stats_dict[column+'_Rank'] = df_stats_dict[column].rank(ascending = False)
            # Set the index to newly created column, Rating_Rank
            #stats_dict.set_index(column+'_Rank')

    #print(df_stats_dict.sort_values('OPS_Weighted'))
    
    
    # Calculate Median Power Rankings
    m = df_stats_dict.groupby(['Player'])[['H_Rank', 'R_Rank','HR_Rank','RBI_Rank','SB_Rank','OPS_Rank']].apply(np.nanmedian)
    m.name = 'Median_Power_Score'
    df_stats_dict = df_stats_dict.join(m, on=['Player'])
    df_stats_dict['Median_Power_Rank'] = df_stats_dict['Median_Power_Score'].rank(ascending = True)

    #Calculate Mean Power Rankings
    df_stats_dict['Mean_Power_Score'] = (df_stats_dict['H_Rank']+df_stats_dict['R_Rank']+df_stats_dict['HR_Rank']+df_stats_dict['RBI_Rank']+df_stats_dict['SB_Rank']+df_stats_dict['OPS_Rank'])/6
    df_stats_dict['Mean_Power_Rank'] = df_stats_dict['Mean_Power_Score'].rank(ascending = True)

    #Average of both ranks
    df_stats_dict['Avg_Rank'] = ((df_stats_dict['Mean_Power_Rank']+df_stats_dict['Median_Power_Rank'])/2).rank(ascending = True)


    # Remove Index Columns
    df_stats_dict = df_stats_dict.iloc[: , 1:]


    return df_stats_dict


def write_mongo(df_stats_dict):
    #Connect to Mongo
    client = pymongo.MongoClient("mongodb+srv://admin:Aggies_1435@cluster0.qj2j8.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    db = client['YahooFantasyBaseball']
    collection = db['draft_analysis']

    #Delete Existing Documents
    myquery = {}
    x = collection.delete_many(myquery)
    print(x.deleted_count, " documents deleted.")


    #Insert New Live Standings
    df_stats_dict.reset_index(inplace=True)
    data_dict = df_stats_dict.to_dict("records")
    collection.insert_many(data_dict)
    client.close()
    


def main():
    setup_tables()
    get_stats()
    df_stats_dict = compile_stats()
    write_mongo(df_stats_dict)

if __name__ == '__main__':
    main()


