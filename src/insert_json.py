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
from datetime_utils import set_last_week
from manager_dict import manager_dict

# Load obfuscated strings from .env file
load_dotenv()    
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')

# Set Up Connections
ca = certifi.where()
client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)

db = client['YahooFantasyBaseball_2023']
collection = db['schedule']

# Assuming you have a list of JSON objects to insert
schedule_json_list = [
    {"Week": {"$numberInt": "10"}, "Team_Number": "1", "Opponent_Team_Number": "9"},
    {"Week": {"$numberInt": "10"}, "Team_Number": "2", "Opponent_Team_Number": "10"},
    {"Week": {"$numberInt": "10"}, "Team_Number": "3", "Opponent_Team_Number": "4"},
    {"Week": {"$numberInt": "10"}, "Team_Number": "4", "Opponent_Team_Number": "3"},
    {"Week": {"$numberInt": "10"}, "Team_Number": "5", "Opponent_Team_Number": "7"},
    {"Week": {"$numberInt": "10"}, "Team_Number": "6", "Opponent_Team_Number": "11"},
    {"Week": {"$numberInt": "10"}, "Team_Number": "7", "Opponent_Team_Number": "5"},
    {"Week": {"$numberInt": "10"}, "Team_Number": "8", "Opponent_Team_Number": "12"},
    {"Week": {"$numberInt": "10"}, "Team_Number": "9", "Opponent_Team_Number": "1"},
    {"Week": {"$numberInt": "10"}, "Team_Number": "10", "Opponent_Team_Number": "2"},
    {"Week": {"$numberInt": "10"}, "Team_Number": "11", "Opponent_Team_Number": "6"},
    {"Week": {"$numberInt": "10"}, "Team_Number": "12", "Opponent_Team_Number": "8"}
]

# Insert the JSON documents into the collection
collection.insert_many(schedule_json_list)
client.close()