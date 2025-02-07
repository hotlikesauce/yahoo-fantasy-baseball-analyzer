import csv,os,time
from pymongo import MongoClient
import certifi
import zipfile
from dotenv import load_dotenv

#Local Modules
from email_utils import *
from mongo_utils import mongo_write_team_IDs

load_dotenv()
MONGO_DB = os.environ.get('MONGO_DB')

#Get Mongo Password from env vars
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
#Connect to Mongo, the ca is for ignoring TLS/SSL handshake issues
ca = certifi.where()
client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)

db = client[MONGO_DB]  # Replace 'your_database_name' with the actual database name

# Retrieve all collection names in the database
collection_names = db.list_collection_names()

def main():
    # Iterate over each collection
    for collection_name in collection_names:
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



    def zip_csv_files(file_list, output_path):
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_name in file_list:
                file_path = file_name + '.csv'  # Append .csv extension to the file name
                if os.path.isfile(file_path):
                    # Add the CSV file to the zip archive
                    zipf.write(file_path, os.path.basename(file_path))
                    # Delete the CSV file
                    os.remove(file_path)

        print(f'Successfully zipped {len(file_list)} CSV files to {output_path}.')


    # Example usage
    output_zip = 'Summertime_Sadness.zip'

    zip_csv_files(collection_names, output_zip)

    send_csvs(output_zip)
    time.sleep(5)
    os.remove('Summertime_Sadness.zip')

if __name__ == '__main__':
    if MONGO_DB == 'YahooFantasyBaseball_2025':
        main()
    else:
        pass
