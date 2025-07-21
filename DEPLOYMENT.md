# AWS Lambda Deployment Guide

This guide will help you deploy your Yahoo Fantasy Baseball analyzer to AWS Lambda with automated scheduling.

## Prerequisites

1. **AWS Account** with appropriate permissions
2. **AWS CLI** installed and configured (`aws configure`)
3. **Node.js** (for AWS CDK)
4. **Python 3.9+**
5. **Docker** (optional, for container deployment)

## Quick Start

### Option 1: Automated CDK Deployment (Recommended)

```bash
# Install Python dependencies
pip install -r requirements.txt

# Run the deployment script
python deploy.py
```

This will:
- Create a Lambda layer with all dependencies
- Deploy two Lambda functions (weekly updates & live standings)
- Set up EventBridge schedules automatically
- Configure proper IAM permissions

### Option 2: Manual CDK Deployment

```bash
# Install CDK globally
npm install -g aws-cdk

# Bootstrap CDK (first time only)
cdk bootstrap

# Create Lambda layer
mkdir -p lambda_layer/python
pip install -r requirements.txt -t lambda_layer/python/
cp -r src lambda_layer/python/

# Deploy the stack
cdk deploy
```

### Option 3: Docker Container Deployment

```bash
# Build and deploy using Docker
docker build -t yahoo-fantasy-lambda .

# Tag for ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com

# Push to ECR and create Lambda function manually in AWS Console
```

## Environment Variables

Make sure your `.env` file in the `src/` directory contains:

```
GMAIL=your-email@gmail.com
GMAIL_PASSWORD=your-app-password
MONGO_CLIENT=your-mongodb-connection-string
YAHOO_LEAGUE_ID=https://baseball.fantasysports.yahoo.com/b1/YOUR_LEAGUE_ID/
MONGO_DB=your-database-name
```

**Important**: The Lambda functions will read these from the deployed code. For production, consider using AWS Secrets Manager or Parameter Store.

## Scheduled Functions

### Weekly Updates Function
- **Schedule**: Every Sunday at 5:00 AM ET
- **Timeout**: 15 minutes
- **Memory**: 512 MB
- **Runs**: All weekly analysis scripts

### Live Standings Function  
- **Schedule**: Every 15 minutes
- **Timeout**: 5 minutes
- **Memory**: 256 MB
- **Runs**: Live standings updates only

## Monitoring

After deployment, monitor your functions:

1. **CloudWatch Logs**: Check function execution logs
2. **CloudWatch Metrics**: Monitor invocations, errors, duration
3. **EventBridge**: Verify scheduled triggers are working

## Troubleshooting

### Common Issues

1. **Import Errors**: Make sure all dependencies are in the Lambda layer
2. **Timeout**: Increase timeout for functions that take longer
3. **Memory**: Increase memory allocation if functions fail
4. **Permissions**: Ensure Lambda has proper IAM permissions

### Debugging

```bash
# Check CDK diff before deployment
cdk diff

# View CloudWatch logs
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/YahooFantasy"

# Test function locally
python -c "from lambda_handler import lambda_handler_weekly; lambda_handler_weekly({}, {})"
```

## Cost Optimization

- **Free Tier**: 1M requests/month and 400,000 GB-seconds compute time
- **Estimated Monthly Cost**: ~$1-5 depending on execution frequency
- **Optimization**: Reduce memory allocation and timeout for better cost efficiency

## Security Best Practices

1. Use AWS Secrets Manager for sensitive data
2. Apply least-privilege IAM policies
3. Enable CloudTrail for audit logging
4. Use VPC endpoints if accessing private resources

## Cleanup

To remove all resources:

```bash
cdk destroy
```

This will delete all Lambda functions, EventBridge rules, and associated resources.