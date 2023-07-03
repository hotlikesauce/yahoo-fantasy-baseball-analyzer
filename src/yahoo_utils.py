import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
import os

MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')

def build_team_numbers(df):

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


