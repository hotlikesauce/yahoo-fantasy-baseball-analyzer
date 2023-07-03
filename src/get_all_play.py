import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
from pymongo import MongoClient
import time, datetime, os
import certifi
from dotenv import load_dotenv
from loguru import logger

# Local Modules - email utils for failure emails, mongo utils to 
from email_utils import send_failure_email
from datetime_utils import set_last_week
from manager_dict import manager_dict
from mongo_utils import *

# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')
MONGO_DB = os.environ.get('MONGO_DB')

def get_all_play(lastWeek):
    # Set week number
    allPlaydf = pd.DataFrame(columns = ['Team','Week','R','H','HR','SB','OPS','RBI','HRA','ERA','WHIP','K9','QS','SVH'])
    for matchup in range(1, 13):
        data = []      
        # Setting this sleep timer on a few weeks helps with the rapid requests to the Yahoo servers
        # If you request the site too much in a short amount of time, you will be blocked temporarily
        time.sleep(5)     
        
        def open_url(url):
            try:
                source = urllib.request.urlopen(url).read()
                return source
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    logger.error("Error 404: Page not found. Retrying...")
                    return open_url(url)  # Recursive call to retry opening the URL
                else:
                    raise e  # Reraise any other HTTP errors
        
        try:            
            source = urllib.request.urlopen(YAHOO_LEAGUE_ID + 'matchup?week=' + str(lastWeek) + '&module=matchup&mid1=' + str(matchup)).read()
            soup = bs.BeautifulSoup(source, 'lxml')
        except urllib.error.HTTPError as e:
            if e.code == 404:
                logger.error("Error 404: Page not found. Retrying...")
                logger.error(YAHOO_LEAGUE_ID + 'matchup?week=' + str(lastWeek) + '&module=matchup&mid1=' + str(matchup))
                source = urllib.request.urlopen(YAHOO_LEAGUE_ID + 'matchup?week=' + str(lastWeek) + '&module=matchup&mid1=' + str(matchup)).read()
            else:
                raise e

        table = soup.find_all('table')
        df = pd.read_html(str(table))[1]
        df['Week'] = lastWeek
        df.columns = df.columns.str.replace('[#,@,&,/,+]', '')
        # RENAME COLUMN FOR HR ALLOWED SINCE IT'S A DUPLICATE OF THE BATTING CATEGORY
        df.rename(columns={df.columns[9]: 'HRA'}, inplace=True)
        
        # Logic below to handle asterisks that happen for % based stats when ties occur
        df['WHIP'] = df['WHIP'].astype(str)
        df['OPS'] = df['OPS'].astype(str)
        df['K9'] = df['K9'].astype(str)
        
        df['WHIP'] = df['WHIP'].map(lambda x: x.rstrip('*'))
        df['OPS'] = df['OPS'].map(lambda x: x.rstrip('*'))
        df['K9'] = df['K9'].map(lambda x: x.rstrip('*'))

        df['WHIP'] = df['WHIP'].replace(['-'], '0.00')
        df['OPS'] = df['OPS'].replace(['-'], '0.00')
        df['K9'] = df['K9'].replace(['-'], '0.00')

        df['WHIP'] = df['WHIP'].astype(float)
        df['OPS'] = df['OPS'].astype(float)
        df['K9'] = df['K9'].astype(float)

        df = df[['Team', 'Week', 'R', 'H', 'HR', 'SB', 'OPS', 'RBI', 'HRA', 'ERA', 'WHIP', 'K9', 'QS', 'SVH']]
        print(df)
        df['Opponent'] = df.loc[1, 'Team']
        print(df)

        allPlaydf = allPlaydf.append(df.loc[0], True)

    print(lastWeek)
    logger.info(lastWeek)

    # Calculate implied win statistics - The person with the most Runs in a week has an implied win of 1.0, because they would defeat every other team in that category.
    # Lowest scoring player has implied wins of 0, which we manually set to avoid dividing by 0
    for column in allPlaydf:
        if column == 'Team' or column == 'Week' or column == 'Opponent':
            pass
        elif column == 'ERA' or column == 'WHIP':
            allPlaydf[column+'_Rank'] = allPlaydf[column].rank(ascending=False) - 1
            # Set the index to newly created column, Rating_Rank
            allPlaydf.set_index(column+'_Rank')
            allPlaydf[column+'_Coeff'] = allPlaydf[column+'_Rank'] / 11
        else:
            allPlaydf[column+'_Rank'] = allPlaydf[column].rank(ascending=True) - 1
            # Set the index to newly created column, Rating_Rank
            allPlaydf.set_index(column+'_Rank')
            allPlaydf[column+'_Coeff'] = allPlaydf[column+'_Rank'] / 11

        coeff_cols = [col for col in allPlaydf.columns if 'Coeff' in col]
        coeff_cols.append('Team')
        coeff_cols.append('Week')
        coeff_cols.append('Opponent')
        rankings_df = allPlaydf[coeff_cols]
    
    cols_to_sum = rankings_df.columns[:df.shape[1]-1]
    rankings_df['Expected_Wins'] = rankings_df[cols_to_sum].sum(axis=1)
    
    # Remove Individual Stat Columns
    rankings_df = rankings_df[['Team', 'Week', 'Opponent', 'Expected_Wins']]
    
    rankings_df_expanded = rankings_df.merge(right=rankings_df, left_on='Team', right_on='Opponent')
    
    rankings_df_expanded = rankings_df_expanded.rename(columns={"Team_x": "Team", "Week_x": "Week","Opponent_x": "Opponent","Expected_Wins_x": "Team_Expected_Wins","Expected_Wins_y": "Opponent_Expected_Wins"})
    rankings_df_expanded = rankings_df_expanded[['Week', 'Team', 'Team_Expected_Wins', 'Opponent', 'Opponent_Expected_Wins']]
    rankings_df_expanded['Matchup_Difference'] = (rankings_df_expanded['Team_Expected_Wins'] - rankings_df_expanded['Opponent_Expected_Wins']).apply(lambda x: round(x, 2))
    rankings_df_expanded['Matchup_Power'] = (rankings_df_expanded['Team_Expected_Wins'] + rankings_df_expanded['Opponent_Expected_Wins']).apply(lambda x: round(x, 2))
    
    print(rankings_df_expanded)
    
    # BUILD TABLE WITH TEAM NAME AND NUMBER
    source = uReq(YAHOO_LEAGUE_ID).read()
    soup = bs.BeautifulSoup(source, 'lxml')

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
        
        # Here contains the Team name and team number
        result_dict = {link_url[-1]: link_text for link_text, link_url in links if link_text != ''}
        print(result_dict)

    # Map team numbers from the dictionary to a new Series
    # Iterate through the rows of the DataFrame
    for index, row in rankings_df_expanded.iterrows():
        team_name = row['Team']
        for link in links:
            if link[0] == team_name:
                team_number = link[1][-2:] if link[1][-2:].isdigit() else link[1][-1:]  # Grab the last 2 characters if they are both digits, else grab the last character
                rankings_df_expanded.at[index, 'Team_Number'] = team_number
                break
    rankings_df_expanded['Manager_Name'] = rankings_df_expanded['Team_Number'].map(manager_dict)
    print(rankings_df_expanded)

    # Reset dfs for new weeks so data isn't aggregated
    del allPlaydf, rankings_df

    return rankings_df_expanded

    


def main():
    logger.add("logs/get_all_play.log", rotation="500 MB")
    try:
        lastWeek = set_last_week()
        rankings_df = get_all_play(lastWeek)
        #db name, collection, query
        clear_mongo_query(MONGO_DB,'coefficient','"Week":'+str(lastWeek))
        #db name, collection, dataframe
        write_mongo(MONGO_DB,rankings_df,'coefficient')
    except Exception as e:
        logger.exception(e)
        send_failure_email(str(e),'get all play')
        raise e


if __name__ == '__main__':
    main()
