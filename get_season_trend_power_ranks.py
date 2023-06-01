from calendar import week
from msilib.schema import Error
import pandas as pd
import urllib
import urllib.request
from urllib.request import urlopen as uReq
import bs4 as bs
from functools import reduce
import certifi
from pymongo import MongoClient
import time,datetime,os
from dotenv import load_dotenv

# Local Modules - email utils for failure emails, mongo utils to 
from email_utils import send_failure_email
from player_dict import player_dict

# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')

def get_records():


    #Actual Records
    source = urllib.request.urlopen(YAHOO_LEAGUE_ID).read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    df_rec = pd.read_html(str(table))[0]
    df_rec=df_rec.rename(columns = {'Team':'Team Name'})
    

    #Batting Records
    source = urllib.request.urlopen(YAHOO_LEAGUE_ID+'headtoheadstats?pt=B&type=record').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    dfb = pd.read_html(str(table))[0]

    #Pitching Records
    source = urllib.request.urlopen(YAHOO_LEAGUE_ID+'headtoheadstats?pt=P&type=record').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    dfp = pd.read_html(str(table))[0]

    #Duplicates for Batting and Hitting
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
    
    #Set week number
    my_date = datetime.date.today()
    year, week_num, day_of_week = my_date.isocalendar()
    if week_num < 30:
        thisWeek = (week_num - 14)
    else:
        thisWeek = (week_num - 15)

    #Batting Stats
    source = urllib.request.urlopen(YAHOO_LEAGUE_ID+'headtoheadstats?pt=B&type=stats').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    dfb = pd.read_html(str(table))[0]

    #Pitching Stats
    source = urllib.request.urlopen(YAHOO_LEAGUE_ID+'headtoheadstats?pt=P&type=stats').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    dfp = pd.read_html(str(table))[0]
    dfp.rename(columns={dfp.columns[1]: 'HRA'},inplace=True)
    
    dfp.columns = dfp.columns.str.replace('[#,@,&,/,+]', '')
    
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
    
    df_merge['Stats_Power_Score'] = (df_merge['R_Rank_Stats']+df_merge['H_Rank_Stats']+df_merge['HR_Rank_Stats']+df_merge['SB_Rank_Stats']+df_merge['OPS_Rank_Stats']+df_merge['RBI_Rank_Stats']+df_merge['ERA_Rank_Stats']+df_merge['WHIP_Rank_Stats']+df_merge['K9_Rank_Stats']+df_merge['QS_Rank_Stats']+df_merge['SVH_Rank_Stats']+df_merge['HRA_Rank_Stats'])/12
    df_merge['Stats_Power_Rank'] = df_merge['Stats_Power_Score'].rank(ascending = True)
    df_merge["Week"]= thisWeek
    
    ##FOR WHEN TEAMS CLINCH PLAYOFFS AND HAVE ASTERISKS NEXT TO THEIR NAMES
    try:        
        df_merge['Rank'] = df_merge['Rank'].str.replace('*','').astype(int)
    except AttributeError:
        print("No one has clinched playoffs yet, yo")
    df_merge['Variation'] = df_merge['Stats_Power_Rank'] - df_merge['Rank'] 


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



    # Map team numbers from the dictionary to a new Series
    #Iterate through the rows of the DataFrame
    for index, row in df_merge.iterrows():
        team_name = row['Team Name']
        for link in links:
            if link[0] == team_name:
                team_number = link[1][-2:] if link[1][-2:].isdigit() else link[1][-1:] # Grab the last 2 characters if they are both digits, else grab the last character
                df_merge.at[index, 'Team_Number'] = team_number
                break
    df_merge['Player_Name'] = df_merge['Team_Number'].map(player_dict)
    #print(df_merge.sort_values(by=['Pct'],ascending=False,ignore_index=True))

    print(df_merge)

    return df_merge

def write_mongo(power_rank_df):
    #Connect to Mongo, the ca is for ignoring TLS/SSL handshake issues
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)
    db = client['YahooFantasyBaseball_2023']
    collection = db['power_ranks_season_trend']

    #Insert New Live Standings
    power_rank_df.reset_index(inplace=True)
    data_dict = power_rank_df.to_dict("records")
    collection.insert_many(data_dict)
    client.close()



def main():
    try:
        records_df = get_records()
        power_rank_df = get_stats(records_df)
        write_mongo(power_rank_df)
    except Exception as e:
        filename = os.path.basename(__file__)
        error_message = str(e)
        send_failure_email(error_message,filename)



if __name__ == '__main__':
    main()