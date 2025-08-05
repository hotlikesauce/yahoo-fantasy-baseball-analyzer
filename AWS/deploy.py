#!/usr/bin/env python3
"""
Deployment script for Yahoo Fantasy Baseball Lambda functions
"""
import os
import subprocess
import shutil
import sys

def run_command(command, cwd=None):
    """Run a shell command and handle errors"""
    print(f"Running: {command}")
    try:
        result = subprocess.run(command, shell=True, check=True, cwd=cwd, 
                              capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {command}")
        print(f"Error: {e.stderr}")
        return False

def create_lambda_layer():
    """Create Lambda layer with Python dependencies"""
    print("Creating Lambda layer...")
    
    # Create layer directory structure
    layer_dir = "lambda_layer"
    python_dir = os.path.join(layer_dir, "python")
    
    if os.path.exists(layer_dir):
        shutil.rmtree(layer_dir)
    
    os.makedirs(python_dir, exist_ok=True)
    
    # Install dependencies one by one to avoid conflicts
    dependencies = [
        "beautifulsoup4==4.11.1",
        "certifi==2022.9.24", 
        "pandas==1.5.1",
        "pymongo==4.2.0",
        "python-dotenv==1.0.0",
        "requests==2.31.0"
    ]
    
    print("Installing dependencies...")
    for dep in dependencies:
        print(f"Installing {dep}...")
        cmd = f'python -m pip install --isolated --no-deps --target "{python_dir}" {dep}'
        if not run_command(cmd):
            print(f"Failed to install {dep}, trying alternative method...")
            # Try with pip install and copy approach
            temp_dir = "temp_install"
            os.makedirs(temp_dir, exist_ok=True)
            alt_cmd = f'python -m pip install --isolated --target "{temp_dir}" {dep}'
            if run_command(alt_cmd):
                # Copy from temp to python_dir
                for item in os.listdir(temp_dir):
                    src_path = os.path.join(temp_dir, item)
                    dst_path = os.path.join(python_dir, item)
                    if os.path.isdir(src_path):
                        if os.path.exists(dst_path):
                            shutil.rmtree(dst_path)
                        shutil.copytree(src_path, dst_path)
                    else:
                        shutil.copy2(src_path, dst_path)
                shutil.rmtree(temp_dir)
            else:
                print(f"Failed to install {dep}")
                return False
    
    # Copy source code to layer
    src_dest = os.path.join(python_dir, "src")
    shutil.copytree("src", src_dest)
    
    print("Lambda layer created successfully!")
    return True

def deploy_cdk():
    """Deploy CDK stack"""
    print("Deploying CDK stack...")
    
    # Install CDK dependencies
    if not run_command("npm install -g aws-cdk"):
        print("Failed to install CDK. Make sure Node.js is installed.")
        return False
    
    # Bootstrap CDK (only needed once per account/region)
    print("Bootstrapping CDK...")
    run_command("cdk bootstrap")
    
    # Deploy the stack
    if not run_command("cdk deploy --require-approval never"):
        return False
    
    print("CDK deployment completed!")
    return True

def main():
    """Main deployment function"""
    print("Starting Yahoo Fantasy Baseball Lambda deployment...")
    
    # Check if AWS CLI is configured
    if not run_command("aws sts get-caller-identity"):
        print("AWS CLI not configured. Please run 'aws configure' first.")
        return False
    
    # Create Lambda layer
    if not create_lambda_layer():
        print("Failed to create Lambda layer")
        return False
    
    # Deploy CDK stack
    if not deploy_cdk():
        print("Failed to deploy CDK stack")
        return False
    
    print("\n🎉 Deployment completed successfully!")
    print("\nYour Lambda functions are now deployed:")
    print("- Weekly Updates: Runs every Sunday at 5am ET")
    print("- Live Standings: Runs every 15 minutes")
    print("\nCheck the AWS Console to monitor your functions.")

if __name__ == "__main__":
    main()