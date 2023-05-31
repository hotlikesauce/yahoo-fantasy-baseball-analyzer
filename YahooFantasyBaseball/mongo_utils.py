import certifi,os,pandas
from pymongo import MongoClient, collection
from dotenv import load_dotenv
load_dotenv()

def mongo_write_team_IDs(df_Standings):
    
    df_teamIDs = df_Standings[['Team', 'Team_Number', 'Player_Name']].copy()    

    #Get Mongo Password from env vars
    MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
    #Connect to Mongo, the ca is for ignoring TLS/SSL handshake issues
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)
    db = client['YahooFantasyBaseball_2023']
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