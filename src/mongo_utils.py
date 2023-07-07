import certifi,os,pandas
from pymongo import MongoClient, collection
from dotenv import load_dotenv
import pandas as pd
import ast
load_dotenv()

#Get Mongo Password from env vars
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')

def mongo_write_team_IDs(db_name,df_Standings):
    
    df_teamIDs = df_Standings[['Team', 'Team_Number']].copy()    
    
    #Connect to Mongo, the ca is for ignoring TLS/SSL handshake issues
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)
    db = client[db_name]
    collection = db['team_dict']

    #Delete Existing Documents
    myquery = {}
    x = collection.delete_many(myquery)
    print(x.deleted_count, " documents deleted.")


    #Insert New Season Standings
    df_teamIDs.reset_index(inplace=True)
    data_dict = df_teamIDs.to_dict("records")
    collection.insert_many(data_dict)
    client.close()

def write_mongo(db_name,df,coll):
    # Set Up Connections
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)
    
    db = client[db_name]
    collection = db[coll]


    # Reset Index and insert entire DF into MondgoDB
    # df.reset_index(inplace=True)
    data_dict = df.to_dict("records")
    collection.insert_many(data_dict)
    client.close()

def clear_mongo(db_name,coll):


    # Set Up Connections
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)
    db = client[db_name]
    collection = db[coll]
    
    #Delete Existing Documents From Last Week Only
    myquery = {}
    x = collection.delete_many(myquery)
    print(x.deleted_count, " documents deleted.")

def clear_mongo_query(db_name,coll,query):

    
    # Set Up Connections
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)
    db = client[db_name]
    collection = db[coll]
    #Delete Existing Documents From Last Week Only
    querymod = "{" + query + "}"
    myquery = ast.literal_eval(querymod)
    x = collection.delete_many(myquery)
    print(x.deleted_count, " documents deleted.")



def get_mongo_data(db_name,coll,query):
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)

    # Access the database and collection
    db = client[db_name]
    collection = db[coll]

    querymod = "{" + query + "}"
    myquery = ast.literal_eval(querymod)

    # Retrieve the data from the collection
    data = list(collection.find(myquery))

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data)

    return df
