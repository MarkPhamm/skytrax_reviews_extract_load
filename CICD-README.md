# British Airways Data Pipeline CI/CD

## Overview

This project includes a comprehensive CI/CD pipeline that automatically **executes the British Airways data extraction and loading pipeline** every **Monday at 12 AM EST** directly in GitHub Actions.

## Pipeline Schedule

- **GitHub Actions**: Runs every Monday at 12 AM EST (`0 5 * * 1` cron expression)
- **Execution**: Directly runs the data pipeline tasks (scraping, transformation, S3 upload, Snowflake load)

## Components

### 1. Updated Airflow DAG
- **File**: `astronomer/dags/main_dag.py`
- **Schedule**: `"0 5 * * 1"` (Monday 12 AM EST / 5 AM UTC)
- **Pipeline**: Scraping → Data Cleaning → S3 Upload → Snowflake Load

### 2. GitHub Actions Workflow
- **File**: `.github/workflows/cicd-pipeline.yml`
- **Features**:
  - Automated testing and validation
  - Security scanning with Trivy
  - Docker image building
  - DAG validation
  - Deployment automation
  - Manual trigger capability

### 3. Enhanced Makefile
- **New Commands**:
  - `make validate-dag`: Validate DAG syntax using Docker
  - `make ci-test`: Run full CI test suite
  - `make lint`: Code linting with flake8
  - `make format`: Code formatting with black and isort
  - `make setup-cicd`: Setup CI/CD pipeline

### 4. Setup Script
- **File**: `scripts/setup-cicd.sh`
- **Purpose**: Automated CI/CD pipeline configuration and validation

## Quick Start

1. **Setup the pipeline**:
   ```bash
   make setup-cicd
   ```

2. **Validate your DAG**:
   ```bash
   make validate-dag
   ```

3. **Run local tests**:
   ```bash
   make ci-test
   ```

4. **Push to GitHub** to trigger the CI/CD pipeline

## GitHub Repository Setup

To fully activate the CI/CD pipeline, add these secrets to your GitHub repository:

### Required Secrets
- `AWS_ACCESS_KEY_ID`: AWS access key for S3 operations
- `AWS_SECRET_ACCESS_KEY`: AWS secret key for S3 operations
- `AWS_DEFAULT_REGION`: AWS region (optional, defaults to us-east-1)
- `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier
- `SNOWFLAKE_USER`: Snowflake username
- `SNOWFLAKE_PASSWORD`: Snowflake password
- `SNOWFLAKE_DATABASE`: Snowflake database name (optional, defaults to BRITISH_AIRWAYS)
- `SNOWFLAKE_SCHEMA`: Snowflake schema name (optional, defaults to RAW_DATA)
- `SNOWFLAKE_WAREHOUSE`: Snowflake warehouse name (optional, defaults to COMPUTE_WH)

### Adding Secrets
1. Go to your GitHub repository
2. Navigate to Settings → Secrets and variables → Actions
3. Click "New repository secret" for each secret above

## Pipeline Triggers

The CI/CD pipeline runs automatically on:

1. **Schedule**: Every Monday at 12 AM EST (executes data pipeline)
2. **Manual**: Via GitHub Actions "Run workflow" button (executes data pipeline)
3. **Push**: When code is pushed to the `main` branch (testing only)
4. **Pull Request**: When PRs are created/updated (testing only)

## Pipeline Stages

### 1. Testing
- Code linting with flake8
- Format checking with black and isort
- DAG syntax validation
- Unit tests (if present)

### 2. Security
- Vulnerability scanning with Trivy
- Security reports uploaded to GitHub Security tab

### 3. Build & Validation
- Docker image building
- Container-based DAG validation

### 4. Data Pipeline Execution
- Step 1: Scrape British Airways data
- Step 2: Transform and clean data  
- Step 3: Upload to S3
- Step 4: Load to Snowflake
- Data artifacts archive (30-day retention)

## Monitoring

- **GitHub Actions**: Check workflow status in the "Actions" tab
- **Pipeline Logs**: Detailed execution logs for each step
- **Data Artifacts**: Download processed CSV files from workflow runs (30-day retention)
- **S3**: Monitor uploaded files in your S3 bucket
- **Snowflake**: Check loaded data in your Snowflake database

## Troubleshooting

### Common Issues

1. **DAG Validation Fails**:
   ```bash
   make validate-dag
   ```

2. **Dependencies Missing**:
   ```bash
   make setup-cicd
   ```

3. **Format Issues**:
   ```bash
   make format
   ```

### Getting Help

- Check workflow logs in GitHub Actions
- Run `make ci-test` locally for debugging
- Validate DAG syntax with `make validate-dag`

## Next Steps

1. ✅ CI/CD pipeline configured
2. ✅ Schedule set to Monday 12 AM EST  
3. ⏳ Add GitHub repository secrets
4. ⏳ Push changes to trigger first run
5. ⏳ Monitor execution and performance

---

**Pipeline Status**: ✅ Ready to deploy
**Schedule**: 🕛 Monday 12 AM EST (Weekly)
**Next Run**: Automatically triggered based on schedule 