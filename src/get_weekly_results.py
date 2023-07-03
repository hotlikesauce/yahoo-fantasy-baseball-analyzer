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
from datetime_utils import *
from manager_dict import manager_dict
from yahoo_utils import *
from mongo_utils import *

# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')
MONGO_DB = os.environ.get('MONGO_DB')

def get_weekly_results_full_season():
    #Set week number
    weekly_results_df = pd.DataFrame()
    lastWeek = set_last_week()
    for week in range(1,lastWeek):
        #Setting this sleep timer on a few weeks helps with the rapid requests to the Yahoo servers
        #If you request the site too much in a short amount of time you will be blocked temporarily  
        time.sleep(7)
        for matchup in range(1,13):
            time.sleep(2)
            data=[]               
            def open_url(url):
                try:
                    source = urllib.request.urlopen(url).read()
                    return source
                except urllib.error.HTTPError as e:
                    if e.code == 404:
                        print("Error 404: Page not found. Retrying...")
                        return open_url(url)  # Recursive call to retry opening the URL
                    else:
                        raise e  # Reraise any other HTTP errors
            try:            
                source = urllib.request.urlopen(YAHOO_LEAGUE_ID+'matchup?week='+str(week)+'&module=matchup&mid1='+str(matchup)).read()
                soup = bs.BeautifulSoup(source,'lxml')
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    print("Error 404: Page not found. Retrying...")
                    print(YAHOO_LEAGUE_ID+'matchup?week='+str(week)+'&module=matchup&mid1='+str(matchup))
                    source = urllib.request.urlopen(YAHOO_LEAGUE_ID+'matchup?week='+str(week)+'&module=matchup&mid1='+str(matchup)).read()
                else:
                    raise e


            table = soup.find_all('table')
            df = pd.read_html(str(table))[1]
            df.columns = df.columns.str.replace('[#,@,&,/,+]', '')
            df['Week']=week
            df=df[['Team','Week','Score']]
            df['Opponent'] = df.loc[1,'Team']
            df['Opponent_Score'] = df.loc[1,'Score']
            
            
            # This is the best way to calculate success rate. Ties will factor out as a 0-0-12 week generates the same score as a 6-6 week, or a 5-5-2 week,4-4-4 week, etc. (6)
            # You can also calculate total score/12 I guess... But we've come way to far for that
            df['Score_Difference'] = df['Score'] - df['Opponent_Score']
            # Calculate the minimum and maximum values of the 'Score_Difference' column
            min_value = -12
            max_value = 12

            # Normalize the 'Score_Difference' column to a range of 0 to 1
            df['Normalized_Score_Difference'] = (df['Score_Difference'] - min_value) / (max_value - min_value)

            weekly_results_df = weekly_results_df.append(df.loc[0], True)
            print(weekly_results_df)


    weekly_results_df = build_team_numbers(weekly_results_df)
    weekly_results_df['Manager_Name'] = weekly_results_df['Team_Number'].map(manager_dict)

    return weekly_results_df 

def get_weekly_results():
    #Set week number
    weekly_results_df = pd.DataFrame()
    lastWeek = set_last_week()

    #Setting this sleep timer on a few weeks helps with the rapid requests to the Yahoo servers
    #If you request the site too much in a short amount of time you will be blocked temporarily  
    for matchup in range(1,13):
        time.sleep(0.5)
        data=[]               
        def open_url(url):
            try:
                source = urllib.request.urlopen(url).read()
                return source
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    print("Error 404: Page not found. Retrying...")
                    return open_url(url)  # Recursive call to retry opening the URL
                else:
                    raise e  # Reraise any other HTTP errors
        try:            
            print(YAHOO_LEAGUE_ID+'matchup?week='+str(lastWeek)+'&module=matchup&mid1='+str(matchup))
            source = urllib.request.urlopen(YAHOO_LEAGUE_ID+'matchup?week='+str(lastWeek)+'&module=matchup&mid1='+str(matchup)).read()
            soup = bs.BeautifulSoup(source,'lxml')
        except urllib.error.HTTPError as e:
            if e.code == 404:
                print("Error 404: Page not found. Retrying...")
                print(YAHOO_LEAGUE_ID+'matchup?week='+str(lastWeek)+'&module=matchup&mid1='+str(matchup))
                source = urllib.request.urlopen(YAHOO_LEAGUE_ID+'matchup?week='+str(lastWeek)+'&module=matchup&mid1='+str(matchup)).read()
            else:
                raise e


        table = soup.find_all('table')
        df = pd.read_html(str(table))[1]
        df.columns = df.columns.str.replace('[#,@,&,/,+]', '')
        df['Week']=lastWeek
        df=df[['Team','Week','Score']]
        df['Opponent'] = df.loc[1,'Team']
        df['Opponent_Score'] = df.loc[1,'Score']
        
        
        #This is the best way to calculate success rate. Ties will factor out as a 0-0-12 week generates the same score as a 6-6 week, or a 5-5-2 week,4-4-4 week, etc. (6)
        df['Score_Difference'] = df['Score'] - df['Opponent_Score']
        # Calculate the minimum and maximum values of the 'Score_Difference' column
        min_value = -12
        max_value = 12

        # Normalize the 'Score_Difference' column to a range of 0 to 1
        df['Normalized_Score_Difference'] = (df['Score_Difference'] - min_value) / (max_value - min_value)

        weekly_results_df = weekly_results_df.append(df.loc[0], True)
        print(weekly_results_df)


    weekly_results_df = build_team_numbers(weekly_results_df)
    weekly_results_df['Manager_Name'] = weekly_results_df['Team_Number'].map(manager_dict)


    return weekly_results_df 

def main():
    try:
        # weekly_results_FS_df = get_weekly_results_full_season()
        # clear_mongo('weekly_results')
        # write_mongo(weekly_results_FS_df, 'weekly_results')

        weekly_results_df = get_weekly_results()
        # clear_mongo('weekly_results')
        write_mongo(MONGO_DB,weekly_results_df, 'weekly_results')

    except Exception as e:
        filename = os.path.basename(__file__)
        error_message = str(e)
        send_failure_email(error_message,filename)

if __name__ == '__main__':
    main()
