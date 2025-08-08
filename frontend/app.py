from flask import Flask, render_template, jsonify
import sys
import os
import json
import pandas as pd
from bson import ObjectId
import numpy as np

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    from mongo_utils import get_mongo_data
except ImportError as e:
    print(f"Import error: {e}")
    # Fallback for deployment
    src_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src')
    sys.path.insert(0, src_path)
    try:
        from mongo_utils import get_mongo_data
    except ImportError as e2:
        print(f"Second import error: {e2}")
        # Create a dummy function if import fails
        def get_mongo_data(db, collection, query):
            return pd.DataFrame()

from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'src', '.env'))
MONGO_DB = os.environ.get('MONGO_DB', 'YahooFantasyBaseball_2025')

print(f"Starting app with MONGO_DB: {MONGO_DB}")
print(f"Environment variables loaded: {bool(os.environ.get('MONGO_CLIENT'))}")

app = Flask(__name__)

def clean_data_for_json(df):
    """Clean DataFrame for JSON serialization by handling ObjectIds and NaN values"""
    if df.empty:
        return []
    
    # Convert DataFrame to dict
    data = df.to_dict('records')
    
    # Clean each record
    cleaned_data = []
    for record in data:
        cleaned_record = {}
        for key, value in record.items():
            try:
                # Skip MongoDB ObjectId fields
                if isinstance(value, ObjectId):
                    continue
                # Handle numpy arrays first (before pd.isna check)
                elif isinstance(value, np.ndarray):
                    cleaned_record[key] = value.tolist()
                # Handle other objects with tolist method
                elif hasattr(value, 'tolist') and callable(getattr(value, 'tolist')):
                    cleaned_record[key] = value.tolist()
                # Handle numpy scalar types
                elif isinstance(value, (np.integer, np.int64)):
                    cleaned_record[key] = int(value)
                elif isinstance(value, (np.floating, np.float64)):
                    cleaned_record[key] = float(value)
                elif isinstance(value, np.bool_):
                    cleaned_record[key] = bool(value)
                # Handle NaN values for scalars only
                elif isinstance(value, (int, float, str, bool, type(None))):
                    if pd.isna(value):
                        cleaned_record[key] = None
                    else:
                        cleaned_record[key] = value
                else:
                    # For any other type, try to convert to string as fallback
                    cleaned_record[key] = str(value)
            except Exception as e:
                # If any conversion fails, skip this field
                print(f"Warning: Skipping field {key} due to conversion error: {e}")
                continue
        cleaned_data.append(cleaned_record)
    
    return cleaned_data

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'message': 'App is running'})

@app.route('/api/weekly-luck')
def weekly_luck_data():
    """API endpoint for weekly luck analysis data"""
    try:
        df = get_mongo_data(MONGO_DB, 'weekly_luck_analysis', '')
        if df.empty:
            return jsonify({'error': 'No weekly luck data found'})
        
        # Clean data for JSON serialization
        data = clean_data_for_json(df)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/remaining-sos')
def remaining_sos_data():
    """API endpoint for remaining strength of schedule data"""
    try:
        df = get_mongo_data(MONGO_DB, 'remaining_sos', '')
        if df.empty:
            return jsonify({'error': 'No SOS data found'})
        
        # Clean data for JSON serialization
        data = clean_data_for_json(df)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/power-rankings')
def power_rankings_data():
    """API endpoint for power rankings data"""
    try:
        df = get_mongo_data(MONGO_DB, 'power_ranks', '')
        if df.empty:
            return jsonify({'error': 'No power rankings data found'})
        
        # Clean data for JSON serialization
        data = clean_data_for_json(df)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/running-normalized-ranks')
def running_normalized_ranks_data():
    """API endpoint for running normalized ranks time series data with team names"""
    try:
        # Get running normalized ranks data
        ranks_df = get_mongo_data(MONGO_DB, 'running_normalized_ranks', '')
        if ranks_df.empty:
            return jsonify({'error': 'No running normalized ranks data found'})
        
        # Get team dictionary for names
        team_dict_df = get_mongo_data(MONGO_DB, 'team_dict', '')
        if not team_dict_df.empty:
            # Ensure Team_Number is same type for joining
            ranks_df['Team_Number'] = ranks_df['Team_Number'].astype(str)
            team_dict_df['Team_Number'] = team_dict_df['Team_Number'].astype(str)
            
            # Join with team names
            merged_df = ranks_df.merge(team_dict_df[['Team_Number', 'Team']], on='Team_Number', how='left')
        else:
            # Fallback if no team_dict
            merged_df = ranks_df.copy()
            merged_df['Team'] = merged_df['Team_Number'].apply(lambda x: f'Team {x}')
        
        # Clean data for JSON serialization
        data = clean_data_for_json(merged_df)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/team-dict')
def team_dict_data():
    """API endpoint for team dictionary/names"""
    try:
        df = get_mongo_data(MONGO_DB, 'team_dict', '')
        if df.empty:
            return jsonify({'error': 'No team dictionary data found'})
        
        # Clean data for JSON serialization
        data = clean_data_for_json(df)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/live-standings')
def live_standings_data():
    """API endpoint for live standings data"""
    try:
        df = get_mongo_data(MONGO_DB, 'live_standings', '')
        if df.empty:
            return jsonify({'error': 'No live standings data found'})
        
        # Clean data for JSON serialization
        data = clean_data_for_json(df)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/test')
def test_data():
    """Test endpoint to check data availability"""
    try:
        collections = ['weekly_luck_analysis', 'remaining_sos', 'power_ranks', 'running_normalized_ranks', 'team_dict']
        status = {}
        
        for collection in collections:
            df = get_mongo_data(MONGO_DB, collection, '')
            status[collection] = {
                'available': not df.empty,
                'records': len(df) if not df.empty else 0,
                'columns': list(df.columns) if not df.empty else []
            }
        
        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)