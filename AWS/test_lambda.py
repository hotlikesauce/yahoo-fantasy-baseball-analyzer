#!/usr/bin/env python3
"""
Test script for Lambda functions
"""
import json
import sys
import os

# Add src to path for testing
sys.path.append('src')

def test_weekly_handler():
    """Test the weekly updates handler structure (import only)"""
    print("Testing weekly updates handler structure...")
    try:
        from lambda_handler import lambda_handler_weekly
        print("✅ Weekly handler imported successfully")
        return True
        
    except Exception as e:
        print(f"Weekly handler import failed: {str(e)}")
        return False

def test_live_standings_handler():
    """Test the live standings handler structure (import only)"""
    print("Testing live standings handler structure...")
    try:
        from lambda_handler import lambda_handler_live_standings
        print("✅ Live standings handler imported successfully")
        return True
        
    except Exception as e:
        print(f"Live standings handler import failed: {str(e)}")
        return False

def test_imports():
    """Test that all required modules can be imported"""
    print("Testing module imports...")
    try:
        # Test core modules
        import pandas as pd
        import pymongo
        import requests
        import bs4
        from dotenv import load_dotenv
        
        # Test local modules (import only, don't call main functions)
        import src.weekly_updates
        import src.get_live_standings
        
        print("All imports successful!")
        return True
        
    except ImportError as e:
        print(f"Import test failed: {str(e)}")
        return False

def main():
    """Run all tests"""
    print("🧪 Running Lambda function tests...\n")
    
    tests = [
        ("Module Imports", test_imports),
        ("Weekly Handler", test_weekly_handler),
        ("Live Standings Handler", test_live_standings_handler)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        success = test_func()
        results.append((test_name, success))
        print(f"✅ PASSED" if success else "❌ FAILED")
    
    print(f"\n📊 Test Results:")
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {status} {test_name}")
    
    print(f"\n{passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Ready for deployment.")
    else:
        print("⚠️  Some tests failed. Check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main()