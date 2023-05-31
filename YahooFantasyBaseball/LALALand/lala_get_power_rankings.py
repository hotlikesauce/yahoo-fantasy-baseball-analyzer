import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
from functools import reduce
import certifi, os
from pymongo import MongoClient
from dotenv import load_dotenv

#Local Modules
from email_utils import send_failure_email
from mongo_utils import mongo_write_team_IDs

load_dotenv()

def get_records():

    #Actual Records
    source = urllib.request.urlopen('https://baseball.fantasysports.yahoo.com/b1/51133').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    df_rec = pd.read_html(str(table))[0]
    df_rec=df_rec.rename(columns = {'Team':'Team Name'})
    

    #Batting Records
    source = urllib.request.urlopen('https://baseball.fantasysports.yahoo.com/b1/51133/headtoheadstats?pt=B&type=record').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    dfb = pd.read_html(str(table))[0]

    #Pitching Records
    source = urllib.request.urlopen('https://baseball.fantasysports.yahoo.com/b1/51133/headtoheadstats?pt=P&type=record').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    dfp = pd.read_html(str(table))[0]
    

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
    dfb['HR'] = list(zip(dfb.HR_Win,dfb.HR_Draw,dfb.HR_Loss))
    dfb['SB'] = list(zip(dfb.SB_Win,dfb.SB_Draw,dfb.SB_Loss))
    dfb['OBP'] = list(zip(dfb.OBP_Win,dfb.OBP_Draw,dfb.OBP_Loss))
    dfb['RBI'] = list(zip(dfb.RBI_Win,dfb.RBI_Draw,dfb.RBI_Loss))

    #convert tuples to ints
    dfb['R'] = tuple(tuple(map(int, tup)) for tup in  dfb['R'])
    dfb['HR'] = tuple(tuple(map(int, tup)) for tup in dfb['HR'])  
    dfb['SB'] = tuple(tuple(map(int, tup)) for tup in dfb['SB'])  
    dfb['OBP'] = tuple(tuple(map(int, tup)) for tup in dfb['OBP'])  
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
    dfp['W'] = list(zip(dfp.W_Win,dfp.W_Draw,dfp.W_Loss))
    dfp['ERA'] = list(zip(dfp.ERA_Win,dfp.ERA_Draw,dfp.ERA_Loss))
    dfp['WHIP'] = list(zip(dfp.WHIP_Win,dfp.WHIP_Draw,dfp.WHIP_Loss))
    dfp['K'] = list(zip(dfp.K_Win,dfp.K_Draw,dfp.K_Loss))
    dfp['SV'] = list(zip(dfp.SV_Win,dfp.SV_Draw,dfp.SV_Loss))
    
   
   #convert tuples to ints
    dfp['W'] = tuple(tuple(map(int, tup)) for tup in dfp['W'])     
    dfp['ERA'] = tuple(tuple(map(int, tup)) for tup in dfp['ERA'] )     
    dfp['WHIP'] = tuple(tuple(map(int, tup)) for tup in dfp['WHIP'])     
    dfp['K'] = tuple(tuple(map(int, tup)) for tup in dfp['K'])      
    dfp['SV'] = tuple(tuple(map(int, tup)) for tup in dfp['SV'])     
    

    df=reduce(lambda x,y: pd.merge(x,y, on='Team Name', how='outer'), [dfb, dfp,df_rec])

    df=df[['Team Name','R','HR','SB','OBP','RBI','W','ERA','WHIP','K','SV','Rank','GB','Moves']]
    
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
    source = urllib.request.urlopen('https://baseball.fantasysports.yahoo.com/b1/51133/headtoheadstats?pt=B&type=stats').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    dfb = pd.read_html(str(table))[0]

    #Pitching Stats
    source = urllib.request.urlopen('https://baseball.fantasysports.yahoo.com/b1/51133/headtoheadstats?pt=P&type=stats').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    dfp = pd.read_html(str(table))[0]
    
    dfp.columns = dfp.columns.str.replace('[#,@,&,/,+]', '')

    df=reduce(lambda x,y: pd.merge(x,y, on='Team Name', how='outer'), [dfb, dfp])
    
    for column in df:
        if column == 'Team Name':
            pass
        elif column == 'ERA' or column == 'WHIP':
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
    
    df_merge['Stats_Power_Score'] = (df_merge['R_Rank_Stats']+df_merge['HR_Rank_Stats']+df_merge['SB_Rank_Stats']+df_merge['OBP_Rank_Stats']+df_merge['RBI_Rank_Stats']+df_merge['ERA_Rank_Stats']+df_merge['WHIP_Rank_Stats']+df_merge['K_Rank_Stats']+df_merge['W_Rank_Stats']+df_merge['SV_Rank_Stats'])/10
    df_merge['Stats_Power_Rank'] = df_merge['Stats_Power_Score'].rank(ascending = True)
    
    
    ##UNCOMMENT THIS WHEN TEAMS CLINCH PLAYOFFS AND HAVE ASTERISKS NEXT TO THEIR NAMES
    try:        
        df_merge['Rank'] = df_merge['Rank'].str.replace('*','').astype(int)
    except AttributeError:
        print("No one has clinched playoffs yet, yo")
    
    df_merge['Variation'] = df_merge['Stats_Power_Rank'] - df_merge['Rank'] 
    # rankings_df = df_merge[["Team Name","Stats_Power_Rank", "Stats_Power_Score"]]

    # Create a new column for the batter rank
    df_merge['batter_rank'] = (df_merge['R_Rank_Stats']+df_merge['HR_Rank_Stats']+df_merge['SB_Rank_Stats']+df_merge['OBP_Rank_Stats']+df_merge['RBI_Rank_Stats'])/5
    
    # Create a new column for the pitcher rank
    df_merge['pitcher_rank'] = (df_merge['ERA_Rank_Stats']+df_merge['WHIP_Rank_Stats']+df_merge['K_Rank_Stats']+df_merge['W_Rank_Stats']+df_merge['SV_Rank_Stats'])/5
    
    

    # print(rankings_df.sort_values(by=['Stats_Power_Rank']))
    # df_merge.to_csv("Results.csv")


    #BUILD TABLE WITH TEAM NAME AND NUMBER
    source = uReq('https://baseball.fantasysports.yahoo.com/b1/51133').read()
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
        #print(result_dict)



    # Map team numbers from the dictionary to a new Series
    #Iterate through the rows of the DataFrame
    # for index, row in df_merge.iterrows():
    #     team_name = row['Team Name']
    #     for link in links:
    #         if link[0] == team_name:
    #             team_number = link[1][-2:] if link[1][-2:].isdigit() else link[1][-1:] # Grab the last 2 characters if they are both digits, else grab the last character
    #             df_merge.at[index, 'Team_Number'] = team_number
    #             break
    # teamDict = {"1": 'Taylor',"2":'Austin',"3":'Kurtis',"4":'Bryant',"5":'Greg',"6":'Josh',"7":'Eric',"8":'David',"9":'Jamie',"10":'Kevin',"11":'Mikey',"12":'Cooch'}
    # df_merge['Player_Name'] = df_merge['Team_Number'].map(teamDict)
    #print(df_merge.sort_values(by=['Pct'],ascending=False,ignore_index=True))




    print(df_merge)
    #time.sleep(5000)





    return df_merge

def write_mongo(power_rank_df):
    #Get Mongo Password from env vars
    MONGO_CLIENT = os.environ.get('MONGO_CLIENT')

    #Connect to Mongo, the ca is for ignoring TLS/SSL handshake issues
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)

    db = client['LALA_YahooFantasyBaseball_2023']
    collection = db['power_ranks']

    #Delete Existing Documents
    myquery = {}
    x = collection.delete_many(myquery)
    print(x.deleted_count, " documents deleted.")


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