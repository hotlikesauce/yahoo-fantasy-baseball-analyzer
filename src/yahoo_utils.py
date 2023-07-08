import pandas as pd
import bs4 as bs
import http
import urllib
import urllib.request
from urllib.request import urlopen as uReq
import os
import time
import urllib.error
from categories_dict import *

from dotenv import load_dotenv 

load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')

def url_requests(url):
    max_retries = 10
    retry_count = 0
    retry = True

    while retry and retry_count < max_retries:
        try:
            # Your code here
            source = urllib.request.urlopen(url).read()
            soup = bs.BeautifulSoup(source, 'lxml')
            time.sleep(2)

            # If the code above succeeds, set retry to False to exit the loop
            retry = False

        except urllib.error.HTTPError as e:
            if e.code == 404:
                print("HTTP Error 404: Not Found. Retrying...")
                time.sleep(5)
                # Add any additional actions or waits here if needed
            else:
                # Handle other HTTP errors if necessary
                print(f"An HTTP error occurred. Error code: {e.code}. Retrying...")
                time.sleep(10)

        except http.client.IncompleteRead:
            print("Incomplete read error occurred. Retrying...")
            time.sleep(5)
            # Add any additional actions or waits here if needed

        retry_count += 1

    return soup

def build_team_numbers(df):

    soup = url_requests(YAHOO_LEAGUE_ID)

    table = soup.find('table')  # Use find() to get the first table

    # Extract all href links from the table, if found
    if table is not None:
        links = []
        for link in table.find_all('a'):  # Find all <a> tags within the table
            link_text = link.text.strip()  # Extract the hyperlink text
            link_url = link['href']  # Extract the href link
            if link_text != '':
                links.append((link_text, link_url))  # Append the hyperlink text and link to the list


    # Map team numbers from the dictionary to a new Series
    #Iterate through the rows of the DataFrame
    for index, row in df.iterrows():
        team_name = row['Team']
        for link in links:
            if link[0] == team_name:
                team_number = link[1][-2:] if link[1][-2:].isdigit() else link[1][-1:] # Grab the last 2 characters if they are both digits, else grab the last character
                df.at[index, 'Team_Number'] = team_number
                break

    return(df)

#Get Number of Teams
def league_size():
    soup = url_requests(YAHOO_LEAGUE_ID)

    table = soup.find_all('table')
    df_seasonRecords = pd.read_html(str(table))[0]

    return len(df_seasonRecords)

def category_size():
    # Batting Records
    soup = url_requests(YAHOO_LEAGUE_ID+'headtoheadstats?pt=B&type=record')
    table = soup.find_all('table')
    # dfp (data frame pitching) will be the list of pitching stat categories you have
    dfb = pd.read_html(str(table))[0]
    dfb = dfb.columns.tolist()
    dfb.pop(0)

    # Pitching Records
    # Set the flag to control the loop
    soup = url_requests(YAHOO_LEAGUE_ID+'headtoheadstats?pt=P&type=record')
    table = soup.find_all('table')
    # dfp (data frame pitching) will be the list of pitching stat categories you have
    dfp = pd.read_html(str(table))[0]
    dfp = dfp.columns.tolist()
    # Remove duplicate team column
    dfp.pop(0)
    # Change to abbreviations

    combined_list = dfb+dfp
    
    return len(combined_list)


#Returns List of Stat Categories 
def league_stats_batting():
    # Get Batting Records by going to stats page
    soup = url_requests(YAHOO_LEAGUE_ID+'headtoheadstats?pt=B&type=record')

    table = soup.find_all('table')
    # dfb (data frame batting) will be the list of pitching stat categories you have
    dfb = pd.read_html(str(table))[0]

    dfb = dfb.columns.tolist()
    
    updated_list = [batting_abbreviations.get(item, item) for item in dfb]

    #Remove Team Name
    updated_list.pop(0)

    return updated_list

def league_stats_pitching():
    # Pitching Records
    soup = url_requests(YAHOO_LEAGUE_ID+'headtoheadstats?pt=P&type=record')

    table = soup.find_all('table')
    
    # dfp (data frame pitching) will be the list of pitching stat categories you have
    dfp = pd.read_html(str(table))[0]

    dfp = dfp.columns.tolist()


    updated_list = [pitching_abbreviations.get(item, item) for item in dfp]
   
    #Remove Team Name
    updated_list.pop(0)

    return updated_list

    
#Returns dfs of Records of Categories
def league_record_pitching_df():
    # Pitching Records
    soup = url_requests(YAHOO_LEAGUE_ID+'headtoheadstats?pt=P&type=record')

    table = soup.find_all('table')
    
    # dfp (data frame pitching) will be the list of pitching stat categories you have
    dfp = pd.read_html(str(table))[0]

    column_names = dfp.columns

   # Iterate over the column names and modify them according to the logic
    for i, column in enumerate(column_names):
        if column in pitching_abbreviations:
            new_column_name = pitching_abbreviations[column]
            column_names.values[i] = new_column_name

    # Update the column names in the DataFrame
    dfp.columns = column_names


    return dfp

def league_record_batting_df():
   # Get Batting Records by going to stats page

    soup = url_requests(YAHOO_LEAGUE_ID+'headtoheadstats?pt=B&type=record')

    table = soup.find_all('table')
    # dfb (data frame batting) will be the list of pitching stat categories you have
    dfb = pd.read_html(str(table))[0]

    column_names = dfb.columns

    # Iterate over the column names
    for i, column in enumerate(column_names):
        if column in batting_abbreviations:
            # Replace the column name with the corresponding value from the dictionary
            column_names.values[i] = batting_abbreviations[column]

    return dfb


#Returns dfs of stats of categories  
def league_stats_batting_df():
    # Get Batting Records by going to stats page
    soup = url_requests(YAHOO_LEAGUE_ID+'headtoheadstats?pt=B&type=stats')

    table = soup.find_all('table')
    # dfb (data frame batting) will be the list of pitching stat categories you have
    dfb = pd.read_html(str(table))[0]

    column_names = dfb.columns

    # Iterate over the column names
    for i, column in enumerate(column_names):
        if column in batting_abbreviations:
            # Replace the column name with the corresponding value from the dictionary
            column_names.values[i] = batting_abbreviations[column]

    return dfb

def league_stats_pitching_df():
    # Get Batting Records by going to stats page
    soup = url_requests(YAHOO_LEAGUE_ID+'headtoheadstats?pt=P&type=stats')

    table = soup.find_all('table')
    # dfb (data frame batting) will be the list of pitching stat categories you have
    dfp = pd.read_html(str(table))[0]

    column_names = dfp.columns

    # Iterate over the column names and modify them according to the logic
    for i, column in enumerate(column_names):
        if column in pitching_abbreviations:
            new_column_name = pitching_abbreviations[column]
            column_names.values[i] = new_column_name

    # Update the column names in the DataFrame
    dfp.columns = column_names


    return dfp

#Format for All_play
def league_stats_all_play_df():
    # Batting Records
    soup = url_requests(YAHOO_LEAGUE_ID+'headtoheadstats?pt=B&type=record')
    table = soup.find_all('table')
    # dfp (data frame pitching) will be the list of pitching stat categories you have
    dfb = pd.read_html(str(table))[0]
    dfb = dfb.columns.tolist()


    # Pitching Records
    # Set the flag to control the loop
    soup = url_requests(YAHOO_LEAGUE_ID+'headtoheadstats?pt=P&type=record')
    table = soup.find_all('table')
    # dfp (data frame pitching) will be the list of pitching stat categories you have
    dfp = pd.read_html(str(table))[0]
    dfp = dfp.columns.tolist()
    # Remove duplicate team column
    dfp.pop(0)
    # Change to abbreviations
    dfp = [pitching_abbreviations.get(item, item) for item in dfp]

    print
    # Iterate over the column names and modify them according to the logic
    for i, column in enumerate(dfp):
        if column in pitching_abbreviations:
            new_column_name = pitching_abbreviations[column]
            dfp.values[i] = new_column_name

    # Update the column names in the DataFrame


    combined_list = dfb+dfp
    combined_list.insert(1, 'Week')

    df = pd.DataFrame(columns=combined_list)
    df = df.rename(columns={'Team Name': 'Team'})
    
    return df




#This is to format the appropriate df for the all play scrape
def league_stats_all_df():
    # Batting Records
    soup = url_requests(YAHOO_LEAGUE_ID+'headtoheadstats?pt=B&type=record')
    table = soup.find_all('table')
    # dfp (data frame pitching) will be the list of pitching stat categories you have
    dfb = pd.read_html(str(table))[0]
    dfb = dfb.columns.tolist()


    # Pitching Records
    # Set the flag to control the loop
    soup = url_requests(YAHOO_LEAGUE_ID+'headtoheadstats?pt=P&type=record')
    table = soup.find_all('table')
    # dfp (data frame pitching) will be the list of pitching stat categories you have
    dfp = pd.read_html(str(table))[0]
    dfp = dfp.columns.tolist()
    # Remove duplicate team column
    dfp.pop(0)
    # Change to abbreviations
    dfp = [pitching_abbreviations.get(item, item) for item in dfp]

    print
    # Iterate over the column names and modify them according to the logic
    for i, column in enumerate(dfp):
        if column in pitching_abbreviations:
            new_column_name = pitching_abbreviations[column]
            dfp.values[i] = new_column_name

    # Update the column names in the DataFrame

    combined_list = dfb+dfp
    combined_list.insert(1, 'Week')

    df = pd.DataFrame(columns=combined_list)
    df = df.rename(columns={'Team Name': 'Team'})
    
    return df