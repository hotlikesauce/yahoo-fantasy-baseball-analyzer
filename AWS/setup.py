#!/usr/bin/env python3
"""
Setup script for Yahoo Fantasy Baseball Lambda deployment
"""
import os
import subprocess
import sys

def check_prerequisites():
    """Check if all prerequisites are installed"""
    print("Checking prerequisites...")
    
    checks = []
    
    # Check Python version
    if sys.version_info >= (3, 9):
        checks.append(("Python 3.9+", True))
    else:
        checks.append(("Python 3.9+", False))
    
    # Check AWS CLI
    try:
        subprocess.run(["aws", "--version"], capture_output=True, check=True)
        checks.append(("AWS CLI", True))
    except (subprocess.CalledProcessError, FileNotFoundError):
        checks.append(("AWS CLI", False))
    
    # Check Node.js (for CDK)
    try:
        subprocess.run(["node", "--version"], capture_output=True, check=True)
        checks.append(("Node.js", True))
    except (subprocess.CalledProcessError, FileNotFoundError):
        checks.append(("Node.js", False))
    
    # Check Docker (optional)
    try:
        subprocess.run(["docker", "--version"], capture_output=True, check=True)
        checks.append(("Docker", True))
    except (subprocess.CalledProcessError, FileNotFoundError):
        checks.append(("Docker (optional)", False))
    
    # Print results
    print("\nPrerequisite Check Results:")
    all_required_met = True
    for name, status in checks:
        status_icon = "✅" if status else "❌"
        print(f"  {status_icon} {name}")
        if not status and "optional" not in name.lower():
            all_required_met = False
    
    return all_required_met

def check_aws_config():
    """Check if AWS is properly configured"""
    print("\nChecking AWS configuration...")
    try:
        result = subprocess.run(["aws", "sts", "get-caller-identity"], 
                              capture_output=True, check=True, text=True)
        print("✅ AWS CLI is configured")
        return True
    except subprocess.CalledProcessError:
        print("❌ AWS CLI not configured. Run 'aws configure' first.")
        return False

def check_env_file():
    """Check if .env file exists and has required variables"""
    print("\nChecking environment configuration...")
    env_path = os.path.join("src", ".env")
    
    if not os.path.exists(env_path):
        print(f"❌ Environment file not found: {env_path}")
        return False
    
    required_vars = [
        "GMAIL", "GMAIL_PASSWORD", "MONGO_CLIENT", 
        "YAHOO_LEAGUE_ID", "MONGO_DB"
    ]
    
    with open(env_path, 'r') as f:
        content = f.read()
    
    missing_vars = []
    for var in required_vars:
        if var not in content:
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        return False
    
    print("✅ Environment file configured")
    return True

def install_dependencies():
    """Install Python dependencies"""
    print("\nInstalling Python dependencies...")
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                      check=True)
        print("✅ Dependencies installed")
        return True
    except subprocess.CalledProcessError:
        print("❌ Failed to install dependencies")
        return False

def main():
    """Main setup function"""
    print("🚀 Yahoo Fantasy Baseball Lambda Setup\n")
    
    # Run all checks
    checks = [
        ("Prerequisites", check_prerequisites),
        ("AWS Configuration", check_aws_config),
        ("Environment File", check_env_file),
        ("Dependencies", install_dependencies)
    ]
    
    all_passed = True
    for check_name, check_func in checks:
        if not check_func():
            all_passed = False
            print(f"\n❌ {check_name} check failed")
        else:
            print(f"\n✅ {check_name} check passed")
    
    print("\n" + "="*50)
    if all_passed:
        print("🎉 Setup completed successfully!")
        print("\nNext steps:")
        print("1. Run 'python test_lambda.py' to test your functions")
        print("2. Run 'python deploy.py' to deploy to AWS")
    else:
        print("⚠️  Setup incomplete. Please fix the issues above.")
        sys.exit(1)

if __name__ == "__main__":
    main()