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
# from pybaseball import playerid_lookup
# from pybaseball import statcast_pitcher

def get_draft():
    draft_df = pd.DataFrame(columns = ['Round','Pick','Player','Team'])
    #Batting Records
    source = urllib.request.urlopen('https://baseball.fantasysports.yahoo.com/b1/11602/draftresults').read()
    soup = bs.BeautifulSoup(source,'lxml')


    table = soup.find_all('table')
    for x in range(0,22):
        df = pd.read_html(str(table))[x]
        df.columns=['Pick','Player','Team']
        df['Round'] = x+1
    
        draft_df = draft_df.append(df)

    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print(draft_df)
        draft_df['Player'] = draft_df['Player'].str.split('(').str[0]
        draft_df['Player'] = draft_df['Player'].str.split('').str[0]
        draft_df['Player'] = draft_df["Player"].str[:-1]
        print(draft_df)
        draft_df.to_csv('2021_Draft.csv')




def main():
    get_draft()

if __name__ == '__main__':
    main()