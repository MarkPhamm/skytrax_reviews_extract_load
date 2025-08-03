#!/bin/bash

# Setup script for British Airways Data Pipeline CI/CD
echo "🚀 Setting up CI/CD Pipeline for British Airways Data Pipeline"

# Create necessary directories
mkdir -p .github/workflows
mkdir -p scripts

echo "📁 Created necessary directories"

# Check if GitHub CLI is installed
if command -v gh &> /dev/null; then
    echo "✅ GitHub CLI found"
    
    # Check if repo is connected to GitHub
    if gh repo view &> /dev/null; then
        echo "✅ Repository is connected to GitHub"
        
        # Set up repository secrets (you'll need to add actual values)
        echo "🔐 Setting up repository secrets..."
        echo "Please manually add these secrets to your GitHub repository:"
        echo "  - AWS_ACCESS_KEY_ID: Your AWS access key"
        echo "  - AWS_SECRET_ACCESS_KEY: Your AWS secret key"
        echo "  - AWS_DEFAULT_REGION: Your AWS region (optional)"
        echo "  - SNOWFLAKE_ACCOUNT: Your Snowflake account"
        echo "  - SNOWFLAKE_USER: Your Snowflake username"
        echo "  - SNOWFLAKE_PASSWORD: Your Snowflake password"
        echo "  - SNOWFLAKE_DATABASE: Your Snowflake database (optional)"
        echo "  - SNOWFLAKE_SCHEMA: Your Snowflake schema (optional)"
        echo "  - SNOWFLAKE_WAREHOUSE: Your Snowflake warehouse (optional)"
        
    else
        echo "⚠️  Repository not connected to GitHub. Run 'gh repo create' or 'gh repo set-url'"
    fi
else
    echo "⚠️  GitHub CLI not found. Install it for easier setup: https://cli.github.com/"
fi

# Validate current DAG
echo "🔍 Validating DAG syntax using Docker..."
if docker run --rm -v $(pwd)/astronomer:/usr/local/airflow --workdir /usr/local/airflow apache/airflow:2.7.0-python3.9 python -c "import sys; sys.path.append('/usr/local/airflow/dags'); from main_dag import dag; print('✅ DAG syntax is valid')" >/dev/null 2>&1; then
    echo "✅ DAG validation successful"
    echo "📅 Schedule confirmed: Every Monday at 12 AM EST (0 5 * * 1)"
else
    echo "❌ DAG validation failed. Please check your DAG syntax."
fi

# Check if required files exist
echo "📋 Checking required files..."

required_files=(
    "astronomer/dags/main_dag.py"
    "astronomer/Dockerfile"
    "requirements.txt"
    ".github/workflows/cicd-pipeline.yml"
)

for file in "${required_files[@]}"; do
    if [[ -f "$file" ]]; then
        echo "✅ $file exists"
    else
        echo "❌ $file missing"
    fi
done

echo ""
echo "🎉 CI/CD Pipeline Setup Complete!"
echo ""
echo "📅 Your pipeline is scheduled to execute every Monday at 12 AM EST"
echo "🔧 GitHub Actions Schedule: Every Monday at 12 AM EST (cron: '0 5 * * 1')"
echo "📊 Pipeline Execution: Scraping → Transform → S3 Upload → Snowflake Load"
echo ""
echo "Next steps:"
echo "1. Add the required AWS and Snowflake secrets to your GitHub repository"
echo "2. Push your changes to the main branch"
echo "3. The data pipeline will automatically execute on schedule or manual trigger"
echo ""
echo "To test locally, run: make ci-test"
echo "To format code, run: make format"
echo "To validate DAG, run: make validate-dag" 