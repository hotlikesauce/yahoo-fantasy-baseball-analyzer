#!/usr/bin/env python3
"""
Fully automated AWS Lambda deployment using AWS CLI
No manual steps required!
"""
import os
import subprocess
import shutil
import zipfile
import json
import time
import stat

def run_command(command, cwd=None):
    """Run a shell command and handle errors"""
    print(f"Running: {command}")
    try:
        result = subprocess.run(command, shell=True, check=True, cwd=cwd, 
                              capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {command}")
        print(f"Error: {e.stderr}")
        return None

def remove_readonly(func, path, _):
    """Error handler for Windows readonly files"""
    os.chmod(path, stat.S_IWRITE)
    func(path)

def safe_rmtree(path):
    """Safely remove directory tree, handling Windows permission issues"""
    if not os.path.exists(path):
        return
    
    try:
        if os.name == 'nt':  # Windows
            shutil.rmtree(path, onerror=remove_readonly)
        else:
            shutil.rmtree(path)
    except Exception as e:
        print(f"Warning: Could not remove {path}: {e}")
        # Try using Windows rmdir command as fallback
        if os.name == 'nt':
            try:
                subprocess.run(f'rmdir /s /q "{path}"', shell=True, check=True)
            except subprocess.CalledProcessError:
                print(f"Failed to remove {path} with rmdir command too")

def create_deployment_package():
    """Create deployment package"""
    print("Creating deployment package...")
    
    deploy_dir = "deployment_package"
    if os.path.exists(deploy_dir):
        safe_rmtree(deploy_dir)
    os.makedirs(deploy_dir)
    
    # Copy source code
    shutil.copytree("src", os.path.join(deploy_dir, "src"))
    shutil.copy("lambda_handler.py", deploy_dir)
    
    # Install dependencies directly to deployment directory
    print("Installing dependencies...")
    
    # Check if pip is available
    if run_command("pip --version") is None:
        print("❌ pip not found. Trying python -m pip...")
        pip_cmd = "python -m pip"
    else:
        pip_cmd = "pip"
    
    # Install to deployment directory using system pip
    deps = ["beautifulsoup4==4.11.1", "certifi==2022.9.24", "pandas==1.5.1", 
            "pymongo==4.2.0", "python-dotenv==1.0.0", "requests==2.31.0",
            "numpy>=1.21.0", "pytz>=2021.1"]
    
    for dep in deps:
        print(f"Installing {dep}...")
        cmd = f'{pip_cmd} install {dep} --target "{deploy_dir}" --upgrade --no-deps --no-user'
        if run_command(cmd) is None:
            print(f"❌ Failed to install {dep}")
            print("Trying without --no-deps...")
            cmd = f'{pip_cmd} install {dep} --target "{deploy_dir}" --upgrade --no-user'
            if run_command(cmd) is None:
                print(f"❌ Still failed to install {dep}")
                return False
    
    # Create ZIP
    zip_path = "lambda_deployment.zip"
    if os.path.exists(zip_path):
        os.remove(zip_path)
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(deploy_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arc_path = os.path.relpath(file_path, deploy_dir)
                zipf.write(file_path, arc_path)
    
    print(f"✅ Created {zip_path} ({os.path.getsize(zip_path) / 1024 / 1024:.1f} MB)")
    return zip_path

def get_account_id():
    """Get AWS account ID"""
    result = run_command("aws sts get-caller-identity --query Account --output text")
    return result

def create_iam_role():
    """Create IAM role for Lambda"""
    print("Creating IAM role...")
    
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    role_name = "yahoo-fantasy-lambda-role"
    
    # Write trust policy to temporary file to avoid JSON escaping issues
    trust_policy_file = "trust-policy.json"
    with open(trust_policy_file, 'w') as f:
        json.dump(trust_policy, f)
    
    # Create role using file
    result = run_command(f'aws iam create-role --role-name {role_name} --assume-role-policy-document file://{trust_policy_file}')
    
    # Clean up temp file
    if os.path.exists(trust_policy_file):
        os.remove(trust_policy_file)
    
    if result is None:
        # Role might already exist, check if it exists
        check_result = run_command(f"aws iam get-role --role-name {role_name}")
        if check_result is None:
            print("❌ Failed to create or find IAM role")
            return None
        print("✅ Role already exists, continuing...")
    else:
        print("✅ Created new IAM role")
    
    # Attach basic execution policy
    attach_result = run_command(f"aws iam attach-role-policy --role-name {role_name} --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
    if attach_result is None:
        print("Policy might already be attached, continuing...")
    
    # Wait for role to be ready
    print("Waiting for role to be ready...")
    time.sleep(15)  # Increased wait time
    
    account_id = get_account_id()
    return f"arn:aws:iam::{account_id}:role/{role_name}"

def create_lambda_function(function_name, handler, zip_path, role_arn, timeout=300, memory=512):
    """Create Lambda function"""
    print(f"Creating Lambda function: {function_name}")
    
    # Delete if exists
    run_command(f"aws lambda delete-function --function-name {function_name}")
    time.sleep(5)
    
    # Create function
    result = run_command(f"""aws lambda create-function \
        --function-name {function_name} \
        --runtime python3.9 \
        --role {role_arn} \
        --handler {handler} \
        --zip-file fileb://{zip_path} \
        --timeout {timeout} \
        --memory-size {memory} \
        --environment Variables="{{PYTHONPATH=/var/runtime:/var/task:/var/task/src}}" """)
    
    return result is not None

def create_eventbridge_rule(rule_name, schedule_expression, function_name):
    """Create EventBridge rule and connect to Lambda"""
    print(f"Creating EventBridge rule: {rule_name}")
    
    # Create rule
    rule_result = run_command(f'aws events put-rule --name {rule_name} --schedule-expression "{schedule_expression}"')
    if rule_result is None:
        print(f"❌ Failed to create EventBridge rule {rule_name}")
        return False
    
    # Add Lambda permission (remove existing first to avoid conflicts)
    account_id = get_account_id()
    source_arn = f"arn:aws:events:us-east-1:{account_id}:rule/{rule_name}"
    
    # Remove existing permission if it exists
    run_command(f"aws lambda remove-permission --function-name {function_name} --statement-id {rule_name}-permission")
    
    # Add new permission
    perm_result = run_command(f"""aws lambda add-permission \
        --function-name {function_name} \
        --statement-id {rule_name}-permission \
        --action lambda:InvokeFunction \
        --principal events.amazonaws.com \
        --source-arn {source_arn}""")
    
    if perm_result is None:
        print(f"❌ Failed to add Lambda permission for {rule_name}")
        return False
    
    # Add target
    function_arn = f"arn:aws:lambda:us-east-1:{account_id}:function:{function_name}"
    targets = [{"Id": "1", "Arn": function_arn}]
    
    # Write targets to temp file to avoid JSON escaping issues
    targets_file = f"{rule_name}-targets.json"
    with open(targets_file, 'w') as f:
        json.dump(targets, f)
    
    target_result = run_command(f'aws events put-targets --rule {rule_name} --targets file://{targets_file}')
    
    # Clean up temp file
    if os.path.exists(targets_file):
        os.remove(targets_file)
    
    if target_result is None:
        print(f"❌ Failed to add EventBridge target for {rule_name}")
        return False
    
    print(f"✅ Successfully created EventBridge rule {rule_name} with target")
    return True

def main():
    """Main deployment function"""
    print("🚀 Fully Automated AWS Lambda Deployment")
    print("="*50)
    
    # Check AWS CLI
    if not run_command("aws sts get-caller-identity"):
        print("❌ AWS CLI not configured. Run 'aws configure' first.")
        return False
    
    # Create deployment package
    zip_path = create_deployment_package()
    if not zip_path:
        print("❌ Failed to create deployment package")
        return False
    
    # Create IAM role
    role_arn = create_iam_role()
    if not role_arn:
        print("❌ Failed to create IAM role")
        return False
    
    # Create Lambda functions
    functions = [
        {
            "name": "yahoo-fantasy-weekly",
            "handler": "lambda_handler.lambda_handler_weekly",
            "timeout": 900,  # 15 minutes
            "memory": 512,
            "schedule": "cron(0 9 ? * SUN *)"  # 5am ET = 9am UTC
        },
        {
            "name": "yahoo-fantasy-live",
            "handler": "lambda_handler.lambda_handler_live_standings", 
            "timeout": 300,  # 5 minutes
            "memory": 256,
            "schedule": "rate(15 minutes)"
        }
    ]
    
    for func in functions:
        if not create_lambda_function(func["name"], func["handler"], zip_path, role_arn, func["timeout"], func["memory"]):
            print(f"❌ Failed to create {func['name']}")
            return False
        
        # Create schedule
        rule_name = f"{func['name']}-schedule"
        create_eventbridge_rule(rule_name, func["schedule"], func["name"])
    
    print("\n🎉 DEPLOYMENT COMPLETED SUCCESSFULLY!")
    print("\n📋 What was created:")
    print("✅ IAM Role: yahoo-fantasy-lambda-role")
    print("✅ Lambda Function: yahoo-fantasy-weekly (runs Sundays 5am ET)")
    print("✅ Lambda Function: yahoo-fantasy-live (runs every 15 minutes)")
    print("✅ EventBridge schedules configured")
    print("\n🔍 Monitor at: https://console.aws.amazon.com/lambda/")
    
    # Cleanup
    if os.path.exists("deployment_package"):
        safe_rmtree("deployment_package")
    
    return True

if __name__ == "__main__":
    main()