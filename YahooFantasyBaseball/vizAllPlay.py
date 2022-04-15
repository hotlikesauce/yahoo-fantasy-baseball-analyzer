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

def mongo_viz():

    #Set Up Connections
    client = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')
    db = client['YahooFBB']
    collection = db['coefficient']

    maxWins = db.collection.find({'Team': 'Texas Fight'}).count()
    print(maxWins)
    #db.CollectionNameGoesHere.aggregate("""{ $group: { _id : null, sum : { $sum: "$Expected_Wins" } } });"""
    client.close()


def main():
    mongo_viz()

if __name__ == '__main__':
    main()