import pandas as pd
import requests
from bs4 import BeautifulSoup
import bs4 as bs
import urllib
import csv
import urllib.request
from functools import reduce
import pymongo, certifi
import time


def get_records():

    #Actual Records
    source = urllib.request.urlopen('https://baseball.fantasysports.yahoo.com/b1/23893').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    df_rec = pd.read_html(str(table))[0]
    df_rec=df_rec.rename(columns = {'Team':'Team Name'})
    

    #Batting Records
    source = urllib.request.urlopen('https://baseball.fantasysports.yahoo.com/b1/23893/headtoheadstats?pt=B&type=record').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    dfb = pd.read_html(str(table))[0]

    #Pitching Records
    source = urllib.request.urlopen('https://baseball.fantasysports.yahoo.com/b1/23893/headtoheadstats?pt=P&type=record').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    dfp = pd.read_html(str(table))[0]
    
    #RENAME COLUMN FOR HR ALLOWED SINCE IT'S A DUPLICATE OF THE BATTING CATEGORY
    dfp.rename(columns={dfp.columns[1]: 'HRA'},inplace=True)


    for column in dfb:
        if str(column) == 'Team Name':
            pass
        else:
            # new data frame with split value columns
            new = dfb[column].str.split("-", n = 2, expand = True)
            
            # making separate first name column from new data frame
            dfb[str(column)+"_Win"]= new[0]
            dfb[str(column)+"_Loss"]= new[1]
            dfb[str(column)+"_Draw"]= new[2]
    
    #make tuples
    dfb['R'] = list(zip(dfb.R_Win,dfb.R_Draw,dfb.R_Loss))
    dfb['H'] = list(zip(dfb.H_Win,dfb.H_Draw,dfb.H_Loss))
    dfb['HR'] = list(zip(dfb.HR_Win,dfb.HR_Draw,dfb.HR_Loss))
    dfb['SB'] = list(zip(dfb.SB_Win,dfb.SB_Draw,dfb.SB_Loss))
    dfb['OPS'] = list(zip(dfb.OPS_Win,dfb.OPS_Draw,dfb.OPS_Loss))
    dfb['RBI'] = list(zip(dfb.RBI_Win,dfb.RBI_Draw,dfb.RBI_Loss))

    #convert tuples to ints
    dfb['R'] = tuple(tuple(map(int, tup)) for tup in  dfb['R'])  
    dfb['H'] = tuple(tuple(map(int, tup)) for tup in dfb['H'])  
    dfb['HR'] = tuple(tuple(map(int, tup)) for tup in dfb['HR'])  
    dfb['SB'] = tuple(tuple(map(int, tup)) for tup in dfb['SB'])  
    dfb['OPS'] = tuple(tuple(map(int, tup)) for tup in dfb['OPS'])  
    dfb['RBI'] = tuple(tuple(map(int, tup)) for tup in dfb['RBI'])  


    for column in dfp:
        if str(column) == 'Team Name':
            pass
        else:
            # new data frame with split value columns
            new = dfp[column].str.split("-", n = 2, expand = True)
            new = new.astype(int)
            # making separate first name column from new data frame

            dfp[str(column)+"_Win"]= new[0]
            dfp[str(column)+"_Loss"]= new[1]
            dfp[str(column)+"_Draw"]= new[2]
            
    dfp.columns = dfp.columns.str.replace('[#,@,&,/,+]', '')

    #make tuples
    dfp['HRA'] = list(zip(dfp.HRA_Win,dfp.HRA_Draw,dfp.HRA_Loss))
    dfp['ERA'] = list(zip(dfp.ERA_Win,dfp.ERA_Draw,dfp.ERA_Loss))
    dfp['WHIP'] = list(zip(dfp.WHIP_Win,dfp.WHIP_Draw,dfp.WHIP_Loss))
    dfp['K9'] = list(zip(dfp.K9_Win,dfp.K9_Draw,dfp.K9_Loss))
    dfp['QS'] = list(zip(dfp.QS_Win,dfp.QS_Draw,dfp.QS_Loss))
    dfp['SVH'] = list(zip(dfp.SVH_Win,dfp.SVH_Draw,dfp.SVH_Loss))
    
   
   #convert tuples to ints
    dfp['HRA'] = tuple(tuple(map(int, tup)) for tup in dfp['HRA'])     
    dfp['ERA'] = tuple(tuple(map(int, tup)) for tup in dfp['ERA'] )     
    dfp['WHIP'] = tuple(tuple(map(int, tup)) for tup in dfp['WHIP'])     
    dfp['K9'] = tuple(tuple(map(int, tup)) for tup in dfp['K9'])     
    dfp['QS'] = tuple(tuple(map(int, tup)) for tup in dfp['QS'])     
    dfp['SVH'] = tuple(tuple(map(int, tup)) for tup in dfp['SVH'])     
    

    df=reduce(lambda x,y: pd.merge(x,y, on='Team Name', how='outer'), [dfb, dfp,df_rec])

    df=df[['Team Name','R','H','HR','SB','OPS','RBI','HRA','ERA','WHIP','K9','QS','SVH','Rank','GB','Moves']]
    
    for column in df:
        if column in ['Team Name','Rank','GB','Moves']:
            pass
        else:
            df[column+'_Rank'] = df[column].rank(ascending = False)
            # Set the index to newly created column, Rating_Rank
            df.set_index(column+'_Rank')
    
    #change col names to be record independent   
    keep_same = {'Team Name','Rank','GB','Moves'}
    df.columns = ['{}{}'.format(c, '' if c in keep_same else '_Record') for c in df.columns]
    
    print(df.sort_values(by=['R_Record'],ascending=False))
    df = df.dropna()
    return df

def get_stats(records_df):

    #Batting Stats
    source = urllib.request.urlopen('https://baseball.fantasysports.yahoo.com/b1/23893/headtoheadstats?pt=B&type=stats').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    dfb = pd.read_html(str(table))[0]

    #Pitching Stats
    source = urllib.request.urlopen('https://baseball.fantasysports.yahoo.com/b1/23893/headtoheadstats?pt=P&type=stats').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    dfp = pd.read_html(str(table))[0]
    
    dfp.columns = dfp.columns.str.replace('[#,@,&,/,+]', '')
    dfp.rename(columns={dfp.columns[1]: 'HRA'},inplace=True)

    df=reduce(lambda x,y: pd.merge(x,y, on='Team Name', how='outer'), [dfb, dfp])
    
    for column in df:
        if column == 'Team Name':
            pass
        elif column == 'ERA' or column == 'WHIP' or column == 'HRA':
            df[column+'_Rank'] = df[column].rank(ascending = True)
            # Set the index to newly created column, Rating_Rank
            df.set_index(column+'_Rank')
        else:
            df[column+'_Rank'] = df[column].rank(ascending = False)
            # Set the index to newly created column, Rating_Rank
            df.set_index(column+'_Rank')
    

    #Change col names to be stats independent
    keep_same = {'Team Name'}
    df.columns = ['{}{}'.format(c, '' if c in keep_same else '_Stats') for c in df.columns]
    
    
    df_merge=reduce(lambda x,y: pd.merge(x,y, on='Team Name', how='outer'), [df, records_df])
    print(df_merge.head())
    
    df_merge['Stats_Power_Score'] = (df_merge['R_Rank_Stats']+df_merge['H_Rank_Stats']+df_merge['HR_Rank_Stats']+df_merge['SB_Rank_Stats']+df_merge['OPS_Rank_Stats']+df_merge['RBI_Rank_Stats']+df_merge['ERA_Rank_Stats']+df_merge['WHIP_Rank_Stats']+df_merge['K9_Rank_Stats']+df_merge['QS_Rank_Stats']+df_merge['SVH_Rank_Stats']+df_merge['HRA_Rank_Stats'])/12
    df_merge['Stats_Power_Rank'] = df_merge['Stats_Power_Score'].rank(ascending = True)
    
    
    ##UNCOMMENT THIS WHEN TEAMS CLINCH PLAYOFFS AND HAVE ASTERISKS NEXT TO THEIR NAMES
    try:        
        df_merge['Rank'] = df_merge['Rank'].str.replace('*','').astype(int)
    except AttributeError:
        print("No one has clinched playoffs yet, yo")
    
    df_merge['Variation'] = df_merge['Stats_Power_Rank'] - df_merge['Rank'] 
    # rankings_df = df_merge[["Team Name","Stats_Power_Rank", "Stats_Power_Score"]]


    # print(rankings_df.sort_values(by=['Stats_Power_Rank']))
    # df_merge.to_csv("Results.csv")

    return df_merge

def write_mongo(power_rank_df):
    #Connect to Mongo, the ca is for ignoring TLS/SSL handshake issues
    ca = certifi.where()
    client = pymongo.MongoClient("mongodb+srv://admin:Aggies_1435@cluster0.qj2j8.mongodb.net/myFirstDatabase?retryWrites=true&w=majority", tlsCAFile=ca)
    db = client['YahooFantasyBaseball_2023']
    collection = db['power_ranks']

    # #Delete Existing Documents
    # myquery = {}
    # x = collection.delete_many(myquery)
    # print(x.deleted_count, " documents deleted.")


    #Insert New Live Standings
    power_rank_df.reset_index(inplace=True)
    data_dict = power_rank_df.to_dict("records")
    collection.insert_many(data_dict)
    client.close()


def main():
    records_df = get_records()
    power_rank_df = get_stats(records_df)
    write_mongo(power_rank_df)



if __name__ == '__main__':
    main()