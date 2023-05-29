import pandas as pd
import requests
from bs4 import BeautifulSoup
import bs4 as bs
import urllib
import csv
import urllib.request
from urllib.request import urlopen as uReq
from functools import reduce
from time import sleep
import numpy as np
import pymongo, certifi
from datetime import datetime
import datetime

week5 = pd.read_csv('S:\\North_Rockies\\Jonah\\GIS\\GIS_V2\\_Scripts - Taylor\\YahooFantasyBaseball_2023\\YahooFantasyBaseball\\Week5_Recalc.csv')
week5['Team_Number'] = week5['Team_Number'].astype(str)
print(week5)

# #Connect to Mongo, the ca is for ignoring TLS/SSL handshake issues
# ca = certifi.where()
# client = pymongo.MongoClient("mongodb+srv://admin:Aggies_1435@cluster0.qj2j8.mongodb.net/myFirstDatabase?retryWrites=true&w=majority", tlsCAFile=ca)
# db = client['YahooFantasyBaseball_2023']
# collection = db['power_ranks_season_trend']

# #Delete Existing Documents
# myquery = {"Week":5}
# x = collection.delete_many(myquery)
# print(x.deleted_count, " documents deleted.")


# #Insert New Live Standings
# week5.reset_index(inplace=True)
# data_dict = week5.to_dict("records")
# collection.insert_many(data_dict)
# client.close()