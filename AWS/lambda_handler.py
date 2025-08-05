import json
import sys
import os

# Add the src directory to Python path so we can import our modules
sys.path.append('/opt/python/src')
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def lambda_handler_weekly(event, context):
    """Lambda handler for weekly updates - runs every Sunday at 5am ET"""
    try:
        from src.weekly_updates import main as weekly_main
        weekly_main()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Weekly updates completed successfully',
                'event': event
            })
        }
    except Exception as e:
        print(f"Error in weekly updates: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Weekly updates failed: {str(e)}',
                'event': event
            })
        }

def lambda_handler_live_standings(event, context):
    """Lambda handler for live standings - runs every 15 minutes"""
    try:
        from src.get_live_standings import main as standings_main
        standings_main()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Live standings updated successfully',
                'event': event
            })
        }
    except Exception as e:
        print(f"Error in live standings: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Live standings failed: {str(e)}',
                'event': event
            })
        }