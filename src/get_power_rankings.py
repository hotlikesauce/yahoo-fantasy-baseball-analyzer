import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
from functools import reduce
from pymongo import MongoClient
import certifi
import os
from dotenv import load_dotenv
from sklearn.preprocessing import MinMaxScaler

# Local Modules - email utils for failure emails, mongo utils to 
from email_utils import send_failure_email
from manager_dict import manager_dict
from mongo_utils import *
from datetime_utils import *

from yahoo_utils import *

# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')


def get_records():
    
    # Get Actual Records by looking up standings table on league home page
    source = urllib.request.urlopen(YAHOO_LEAGUE_ID).read()
    soup = bs.BeautifulSoup(source,'lxml')
    table = soup.find_all('table')
    df_rec = pd.read_html(str(table))[0]
    df_rec=df_rec.rename(columns = {'Team':'Team Name'})
    

    # Get Batting Records by going to stats page
    source = urllib.request.urlopen(YAHOO_LEAGUE_ID+'headtoheadstats?pt=B&type=record').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    # dfb (data frame batting) will be the list of pitching stat categories you have
    dfb = pd.read_html(str(table))[0]

    # Pitching Records
    source = urllib.request.urlopen(YAHOO_LEAGUE_ID+'headtoheadstats?pt=P&type=record').read()
    soup = bs.BeautifulSoup(source,'lxml')

    table = soup.find_all('table')
    
    # dfp (data frame pitching) will be the list of pitching stat categories you have
    dfp = pd.read_html(str(table))[0]
    
    # RENAME COLUMN FOR HR ALLOWED SINCE IT'S A DUPLICATE OF THE BATTING CATEGORY
    dfp.rename(columns={dfp.columns[1]: 'HRA'},inplace=True)

    # split up columns into W-L-D
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
    
    # make pitching tuples
    dfb['R'] = list(zip(dfb.R_Win,dfb.R_Draw,dfb.R_Loss))
    dfb['H'] = list(zip(dfb.H_Win,dfb.H_Draw,dfb.H_Loss))
    dfb['HR'] = list(zip(dfb.HR_Win,dfb.HR_Draw,dfb.HR_Loss))
    dfb['SB'] = list(zip(dfb.SB_Win,dfb.SB_Draw,dfb.SB_Loss))
    dfb['OPS'] = list(zip(dfb.OPS_Win,dfb.OPS_Draw,dfb.OPS_Loss))
    dfb['RBI'] = list(zip(dfb.RBI_Win,dfb.RBI_Draw,dfb.RBI_Loss))

    # convert tuples to ints
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

    # make pitching tuples
    dfp['HRA'] = list(zip(dfp.HRA_Win,dfp.HRA_Draw,dfp.HRA_Loss))
    dfp['ERA'] = list(zip(dfp.ERA_Win,dfp.ERA_Draw,dfp.ERA_Loss))
    dfp['WHIP'] = list(zip(dfp.WHIP_Win,dfp.WHIP_Draw,dfp.WHIP_Loss))
    dfp['K9'] = list(zip(dfp.K9_Win,dfp.K9_Draw,dfp.K9_Loss))
    dfp['QS'] = list(zip(dfp.QS_Win,dfp.QS_Draw,dfp.QS_Loss))
    dfp['SVH'] = list(zip(dfp.SVH_Win,dfp.SVH_Draw,dfp.SVH_Loss))
    
   
   # convert tuples to ints
    dfp['HRA'] = tuple(tuple(map(int, tup)) for tup in dfp['HRA'])     
    dfp['ERA'] = tuple(tuple(map(int, tup)) for tup in dfp['ERA'] )     
    dfp['WHIP'] = tuple(tuple(map(int, tup)) for tup in dfp['WHIP'])     
    dfp['K9'] = tuple(tuple(map(int, tup)) for tup in dfp['K9'])     
    dfp['QS'] = tuple(tuple(map(int, tup)) for tup in dfp['QS'])     
    dfp['SVH'] = tuple(tuple(map(int, tup)) for tup in dfp['SVH'])     
    

    df=reduce(lambda x,y: pd.merge(x,y, on='Team Name', how='outer'), [dfb, dfp,df_rec])

    # define columns for next df
    df=df[['Team Name','R','H','HR','SB','OPS','RBI','HRA','ERA','WHIP','K9','QS','SVH','Rank','GB','Moves']]
    
    # Create a team ranking based on records in all stat categories
    for column in df:
        if column in ['Team Name','Rank','GB','Moves']:
            pass
        else:
            df[column+'_Rank'] = df[column].rank(ascending = False)
            # Set the index to newly created column, Rating_Rank
            df.set_index(column+'_Rank')
    
    # change col names to be record independent   
    keep_same = {'Team Name','Rank','GB','Moves'}
    df.columns = ['{}{}'.format(c, '' if c in keep_same else '_Record') for c in df.columns]
    
    df = df.dropna()
    return df

def get_stats(records_df):

    # Get Batting Stats
    with urllib.request.urlopen(YAHOO_LEAGUE_ID + 'headtoheadstats?pt=B&type=stats') as response:
        source = response.read()
        encoding = response.headers.get_content_charset()
        html_content = source.decode(encoding)

    soup = bs.BeautifulSoup(html_content, 'lxml')

    table = soup.find_all('table')
    dfb = pd.read_html(str(table))[0]

    # Get Pitching Stats
    with urllib.request.urlopen(YAHOO_LEAGUE_ID + 'headtoheadstats?pt=P&type=stats') as response:
        source = response.read()
        encoding = response.headers.get_content_charset()
        html_content = source.decode(encoding)

    soup = bs.BeautifulSoup(html_content, 'lxml')

    table = soup.find_all('table')
    dfp = pd.read_html(str(table))[0]

    # In case your team names get squirrely 
    dfp.columns = dfp.columns.str.replace('[#,@,&,/,+]', '')
    print(dfp)
    print(dfb)
    # Rename HR to HRA since it's a duplicate of HR on the batting side
    dfp.rename(columns={dfp.columns[1]: 'HRA'},inplace=True)
    df=reduce(lambda x,y: pd.merge(x,y, on='Team Name', how='outer'), [dfb, dfp])
    print(df)


    for column in df:
        if column == 'Team Name':
            pass
        # ERA, WHIP, and HRA need to be ranked descending
        elif column == 'ERA' or column == 'WHIP' or column == 'HRA':
            df[column+'_Rank'] = df[column].rank(ascending = True)
            # Set the index to newly created column, Rating_Rank
            df.set_index(column+'_Rank')
        # All others ranked ascending
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
    
    
    # Teams will clinch playoffs and you need to remove the asterisks next to their names
    try:        
        df_merge['Rank'] = df_merge['Rank'].str.replace('*','').astype(int)
    except AttributeError:
        print("No one has clinched playoffs yet, yo")
    
    # Variation is the difference between your stat ranking and you actual ranking
    df_merge['Variation'] = df_merge['Stats_Power_Rank'] - df_merge['Rank'] 

    # Create a new column for the batter rank
    df_merge['batter_rank'] = (df_merge['R_Rank_Stats']+df_merge['H_Rank_Stats']+df_merge['HR_Rank_Stats']+df_merge['SB_Rank_Stats']+df_merge['OPS_Rank_Stats']+df_merge['RBI_Rank_Stats'])/6
    
    # Create a new column for the pitcher rank
    df_merge['pitcher_rank'] = (df_merge['ERA_Rank_Stats']+df_merge['WHIP_Rank_Stats']+df_merge['K9_Rank_Stats']+df_merge['QS_Rank_Stats']+df_merge['SVH_Rank_Stats']+df_merge['HRA_Rank_Stats'])/6
    
    df_merge = df_merge.rename(columns={'Team Name': 'Team'})

    df_merge_teams = build_team_numbers(df_merge)  
    df_merge_teams['Manager_Name'] = df_merge_teams['Team_Number'].map(manager_dict)
    
    print(df_merge_teams)

    return df_merge_teams
        
# Normalized Ranks 
def get_normalized_ranks(power_rank_df):
    #power_rank_df['Overall'] = 1200 - (power_rank_df['Stats_Power_Score'] * 100)
 
    # Columns to analyze
    high_columns_to_analyze = ['R_Stats', 'H_Stats', 'HR_Stats', 'RBI_Stats', 'SB_Stats', 'OPS_Stats','K9_Stats', 'QS_Stats', 'SVH_Stats' ]

    low_columns_to_analyze = ['ERA_Stats', 'WHIP_Stats', 'HRA_Stats']

    # Calculate Score for each column grouped by team_number
    for column in high_columns_to_analyze:
        min_score = 0  # Set the desired minimum Score value
        min_value = power_rank_df[column].min()
        max_value = power_rank_df[column].max()
        
        scaler = MinMaxScaler(feature_range=(min_score, 100))
        
        # Calculate and assign the scaled Score values
        power_rank_df[column + '_Score'] = scaler.fit_transform(power_rank_df[column].values.reshape(-1, 1))    
    
    # Calculate Score for each LOW column grouped by team_number
    for column in low_columns_to_analyze:
        min_score = 0  # Set the desired minimum Score value
        min_value = power_rank_df[column].min()
        max_value = power_rank_df[column].max()
        
        scaler = MinMaxScaler(feature_range=(min_score, 100))
        
        # Calculate and assign the scaled Score values
        scaled_values = 100 - ((power_rank_df[column] - min_value) / (max_value - min_value)) * 80
        power_rank_df[column + '_Score'] = scaled_values

    # Get the list of Score columns
    score_columns = [column + '_Score' for column in high_columns_to_analyze + low_columns_to_analyze]

    # Sum the Score columns
    power_rank_df['Score_Sum'] = power_rank_df[score_columns].sum(axis=1)
    power_rank_df['Score_Rank'] = power_rank_df['Score_Sum'].rank(ascending=False)
    power_rank_df['Score_Variation'] = power_rank_df['Score_Rank'] - power_rank_df['Rank']

    print(power_rank_df)
    return power_rank_df

def running_normalized_ranks(week_df):
    #week_3_df['Overall'] = 1200 - (week_3_df['Stats_Power_Score'] * 100)
    # Columns to analyze
    high_columns_to_analyze = ['R_Stats', 'H_Stats', 'HR_Stats', 'RBI_Stats', 'SB_Stats', 'OPS_Stats','K9_Stats', 'QS_Stats', 'SVH_Stats' ]

    low_columns_to_analyze = ['ERA_Stats', 'WHIP_Stats', 'HRA_Stats']

    # Calculate Score for each column grouped by team_number
    for column in high_columns_to_analyze:
        min_score = 0  # Set the desired minimum Score value
        min_value = week_df[column].min()
        max_value = week_df[column].max()
        
        scaler = MinMaxScaler(feature_range=(min_score, 100))
        
        # Calculate and assign the scaled Score values
        week_df[column + '_Score'] = scaler.fit_transform(week_df[column].values.reshape(-1, 1))    
    
    # Calculate Score for each LOW column grouped by team_number
    for column in low_columns_to_analyze:
        min_score = 0  # Set the desired minimum Score value
        min_value = week_df[column].min()
        max_value = week_df[column].max()
        
        scaler = MinMaxScaler(feature_range=(min_score, 100))
        
        # Calculate and assign the scaled Score values
        scaled_values = 100 - ((week_df[column] - min_value) / (max_value - min_value)) * 80
        week_df[column + '_Score'] = scaled_values

    # Get the list of Score columns
    score_columns = [column + '_Score' for column in high_columns_to_analyze + low_columns_to_analyze]

    # Sum the Score columns
    week_df['Score_Sum'] = week_df[score_columns].sum(axis=1)
    week_df['Score_Rank'] = week_df['Score_Sum'].rank(ascending=False)
    #week_df['Score_Variation'] = week_df['Score_Rank'] - week_df['Rank']

    return week_df



def main():
    try:
        records_df = get_records()
        power_rank_df = get_stats(records_df)
        clear_mongo('Power_Ranks')
        write_mongo(power_rank_df,'Power_Ranks')

        empty_dict = {}
        power_rank_df = get_mongo_data('Power_Ranks',empty_dict)
        power_rank_season_df = get_mongo_data('power_ranks_season_trend',empty_dict)
        schedule_df = get_mongo_data('schedule',empty_dict)

        normalized_ranks_df = get_normalized_ranks(power_rank_df)
        clear_mongo('normalized_ranks')
        write_mongo(normalized_ranks_df,'normalized_ranks')

        this_week = set_this_week()
        running_normalized_ranks_df = pd.DataFrame()
        clear_mongo('running_normalized_ranks')

        #Calculate full season Normalized Stat Rankings based on past weeks and teams' stats cumulatively at those weeks
        for week in range(1,(this_week)):
            week_dict =  {"Week":week}
            stats_week = get_mongo_data('power_ranks_season_trend',week_dict)
            print(stats_week)
            running_normalized_ranks_df = running_normalized_ranks(stats_week)
            write_mongo(running_normalized_ranks_df,'running_normalized_ranks')
        

    except Exception as e:
        filename = os.path.basename(__file__)
        error_message = str(e)
        print(error_message)
        send_failure_email(error_message,filename)




if __name__ == '__main__':
    main()