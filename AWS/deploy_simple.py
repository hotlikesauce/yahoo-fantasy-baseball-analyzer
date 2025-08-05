#!/usr/bin/env python3
"""
Simple deployment script for Windows - creates deployment package
"""
import os
import shutil
import zipfile
import subprocess

def create_deployment_package():
    """Create a simple ZIP deployment package"""
    print("Creating deployment package...")
    
    # Create deployment directory
    deploy_dir = "deployment_package"
    if os.path.exists(deploy_dir):
        shutil.rmtree(deploy_dir)
    os.makedirs(deploy_dir)
    
    # Copy your source code
    print("Copying source files...")
    shutil.copytree("src", os.path.join(deploy_dir, "src"))
    shutil.copy("lambda_handler.py", deploy_dir)
    
    # Install dependencies directly to deployment directory
    print("Installing dependencies...")
    try:
        # Use a virtual environment approach to avoid pip conflicts
        subprocess.run([
            "python", "-m", "pip", "install", 
            "--target", deploy_dir,
            "--no-user",
            "beautifulsoup4==4.11.1",
            "certifi==2022.9.24", 
            "pandas==1.5.1",
            "pymongo==4.2.0",
            "python-dotenv==1.0.0",
            "requests==2.31.0"
        ], check=True)
        print("Dependencies installed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Failed to install dependencies: {e}")
        return False
    
    # Create ZIP file for Lambda
    print("Creating ZIP package...")
    zip_path = "lambda_deployment.zip"
    if os.path.exists(zip_path):
        os.remove(zip_path)
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(deploy_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arc_path = os.path.relpath(file_path, deploy_dir)
                zipf.write(file_path, arc_path)
    
    print(f"✅ Deployment package created: {zip_path}")
    print(f"📦 Package size: {os.path.getsize(zip_path) / 1024 / 1024:.1f} MB")
    
    return True

def print_manual_instructions():
    """Print manual deployment instructions"""
    print("\n" + "="*60)
    print("🚀 MANUAL DEPLOYMENT INSTRUCTIONS")
    print("="*60)
    print("\n1. Go to AWS Lambda Console:")
    print("   https://console.aws.amazon.com/lambda/")
    
    print("\n2. Create Weekly Updates Function:")
    print("   - Click 'Create function'")
    print("   - Name: yahoo-fantasy-weekly")
    print("   - Runtime: Python 3.9")
    print("   - Upload lambda_deployment.zip")
    print("   - Handler: lambda_handler.lambda_handler_weekly")
    print("   - Timeout: 15 minutes")
    print("   - Memory: 512 MB")
    
    print("\n3. Create Live Standings Function:")
    print("   - Click 'Create function'")
    print("   - Name: yahoo-fantasy-live")
    print("   - Runtime: Python 3.9") 
    print("   - Upload lambda_deployment.zip")
    print("   - Handler: lambda_handler.lambda_handler_live_standings")
    print("   - Timeout: 5 minutes")
    print("   - Memory: 256 MB")
    
    print("\n4. Set up EventBridge Schedules:")
    print("   - Go to EventBridge Console")
    print("   - Create rule for weekly: cron(0 9 ? * SUN *)")
    print("   - Create rule for live: rate(15 minutes)")
    print("   - Target both rules to respective Lambda functions")
    
    print("\n5. Test your functions in the Lambda console")
    print("\n✅ Your lambda_deployment.zip is ready to upload!")

def main():
    """Main function"""
    print("🎯 Simple Lambda Deployment for Windows")
    print("This creates a ZIP file you can upload manually to AWS Lambda\n")
    
    if create_deployment_package():
        print_manual_instructions()
    else:
        print("❌ Deployment package creation failed")

if __name__ == "__main__":
    main()