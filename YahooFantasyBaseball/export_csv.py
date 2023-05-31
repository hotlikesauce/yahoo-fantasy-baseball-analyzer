import csv,os
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv

#Local Modules
from email_utils import send_failure_email
from mongo_utils import mongo_write_team_IDs

load_dotenv()

#Get Mongo Password from env vars
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
#Connect to Mongo, the ca is for ignoring TLS/SSL handshake issues
ca = certifi.where()
client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)
db = client['YahooFantasyBaseball_2023']

# List of collections to export
collections = ['coefficient', 'power_ranks', 'power_ranks_season_trend', 'standings_season_trend']

# Iterate over each collection
for collection_name in collections:
    collection = db[collection_name]

    # Retrieve documents from the collection
    documents = collection.find()

    # Specify the CSV file path
    csv_file_path = f'{collection_name}.csv'

    # Open the CSV file in write mode
    with open(csv_file_path, 'w', newline='',encoding='utf-8') as csvfile:
        # Create a CSV writer object
        writer = csv.writer(csvfile)

        # Write the header row
        writer.writerow(documents[0].keys())

        # Write the data rows
        for document in documents:
            writer.writerow(document.values())

    print(f"Export of {collection_name} completed successfully.")