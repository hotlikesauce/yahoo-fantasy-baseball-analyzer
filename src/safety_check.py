#!/usr/bin/env python3
"""
Safety check module to prevent accidental database operations
"""
import os

def is_safe_to_run():
    """Check if it's safe to run database operations"""
    # Check if we're in a test environment
    if os.environ.get('LAMBDA_TEST_MODE') == 'true':
        print("🛡️ Test mode detected - skipping database operations")
        return False
    
    # Check if this is a Lambda environment
    if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
        print("☁️ Lambda environment detected - proceeding with operations")
        return True
    
    # Local environment - ask for confirmation
    print("⚠️ Running locally - database operations will execute")
    return True

def safe_clear_mongo(db_name, collection):
    """Safely clear mongo collection with checks"""
    if not is_safe_to_run():
        print(f"🛡️ Skipping clear_mongo for {collection}")
        return
    
    from mongo_utils import clear_mongo
    clear_mongo(db_name, collection)

def safe_write_mongo(db_name, df, collection):
    """Safely write to mongo with checks"""
    if not is_safe_to_run():
        print(f"🛡️ Skipping write_mongo for {collection}")
        return
    
    from mongo_utils import write_mongo
    write_mongo(db_name, df, collection)