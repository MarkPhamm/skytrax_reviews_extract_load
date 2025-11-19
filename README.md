# Skytrax Reviews Extract-Load Pipeline

This project implements a comprehensive Extract-Load pipeline for airline review data, designed to process and analyze customer review data from **Skytrax** ([AirlineQuality.com](https://www.airlinequality.com/)). The pipeline extracts **100,000+ reviews from 500+ airlines worldwide**, leveraging **Apache Airflow**, **Snowflake**, **AWS S3**, and **Docker** to load data into Snowflake before transformation using **dbt**-.

---

## 🗂 Project Structure

```
.
├── airflow/             # Airflow configuration and DAGs
│   ├── dags/            # Airflow DAG definitions
│   ├── tasks/           # Custom task implementations
│   ├── plugins/         # Custom Airflow plugins
│   └── logs/            # Airflow execution logs
├── astronomer/          # Astronomer Directory
├── data/                # Data files
│   └── raw_data.csv     # Source data file
├── docker/              # Docker configuration
│   ├── docker-compose.yaml
│   └── Dockerfile
├── .env                 # Environment variables
├── requirements.txt     # Python dependencies
└── Makefile             # Project automation commands
```

---

## ⚙️ Technology Stack

- **Data Source**: Skytrax ([AirlineQuality.com](https://www.airlinequality.com/)) - 500+ airlines
- **Data Processing**: Python 3.12 with Pandas, BeautifulSoup, Requests
- **Workflow Orchestration**: Apache Airflow, Astronomer
- **Data Warehouse**: Snowflake
- **Data Lake**: AWS S3 for staging
- **Containerization**: Docker

---

## Getting Started

### a) Local development

- Install Docker Desktop and the Astro CLI (`astro`).
- From the project root, run: `astro dev start`.
- Open the Airflow UI at `http://localhost:8080` (default user/password `airflow` / `airflow` unless changed).
- In the Airflow UI, go to **Admin → Connections** and configure:
  - `aws_s3_connection`
    - Conn Id: `aws_s3_connection`
    - Conn Type: `Amazon Web Services`
    - Login: your `AWS_ACCESS_KEY_ID`
    - Password: your `AWS_SECRET_ACCESS_KEY`
    - Extra: `{"region_name": "us-east-1"}` (or your AWS region)
  - `snowflake_default`
    - Conn Id: `snowflake_default`
    - Conn Type: `Snowflake`
    - Account: your Snowflake account (e.g. `abc12345.us-east-1`)
    - User: your Snowflake user
    - Password: your Snowflake password
    - Database: `SKYTRAX_REVIEWS_DB`
    - Schema: `RAW`
    - Warehouse: your Snowflake warehouse
- In the Airflow UI, enable and trigger `main_dag` to run the end‑to‑end extract‑load pipeline.

### b) GitHub Actions

- Push this project to a GitHub repository.
- In GitHub, go to **Settings → Secrets and variables → Actions** and add these repository secrets:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_DEFAULT_REGION` (e.g. `us-east-1`)
  - `SNOWFLAKE_ACCOUNT`
  - `SNOWFLAKE_USER`
  - `SNOWFLAKE_PASSWORD`
  - `SNOWFLAKE_DATABASE` (e.g. `SKYTRAX_REVIEWS_DB`)
  - `SNOWFLAKE_SCHEMA` (e.g. `RAW`)
  - `SNOWFLAKE_WAREHOUSE`
- Once secrets are configured, the workflow `.github/workflows/cicd-pipeline.yml` will:
  - run tests and validations on push/PR, and
  - execute the full data pipeline on the scheduled run (every Monday 12 AM EST) or when manually triggered.

## 🧱 Data Architecture

### 1. Data Source
The project processes comprehensive airline review data scraped from **Skytrax (AirlineQuality.com)**, which contains detailed information about customer flight experiences across **500+ airlines worldwide**.

### 2. Data Processing Pipeline
1. **Data Crawling**
   - Automatically discover and crawl customer reviews from all airlines listed on Skytrax
   - Extract **100,000+ reviews** from **500+ airlines** including major carriers worldwide
   - Store raw data with airline identification in `raw_data.csv`
   - Features intelligent pagination and error handling for robust data collection

2. **Data Cleaning & Transformation**
   - Process and clean the raw multi-airline data
   - Standardize formats across different airline review structures
   - Handle missing values and normalize rating systems
   - Generate cleaned dataset with consistent schema

3. **Staging in S3**
   - Upload cleaned multi-airline data to AWS S3 bucket (`upload_cleaned_data_to_s3`)
   - Store in staging area for Snowflake ingestion
   - Maintain data versioning and audit trail
   - Support large-scale data processing with partitioning by airline

4. **Snowflake Loading**
   - Use Snowflake COPY operator to load data from S3
   - Transform and load into target tables with airline-specific schemas
   - Implement incremental loading strategy for continuous data updates
   - Support analytics across multiple airlines and comparative analysis

### 3. Data Quality Framework
- Multi-airline data validation checks
- Comprehensive error handling and logging across all airline sources
- Pipeline monitoring and alerting for large-scale operations
- Snowflake data quality monitoring with airline-specific metrics

---

## 🧩 Project Components

### 📊 Airflow DAGs
Located in `airflow/dags/`:
- DAG definitions for multi-airline data processing workflows
- Task scheduling and dependency management for large-scale operations
- Error handling and retry logic for reliable data extraction
- Snowflake data loading and transformation tasks for all airlines

### 🛠 Custom Tasks
Located in `airflow/tasks/`:
- **Multi-airline scraper**: Automated discovery and extraction from 500+ airlines
- **Data processing**: Transformation logic for diverse airline data formats
- **S3 upload operations**: Large-scale data transfer and storage
- **Snowflake operations**: Data loading and unloading for analytics
- **Custom operators**: Business logic for airline review processing
- **Utility functions**: Data handling for high-volume operations

### 🔌 Airflow Plugins
Located in `airflow/plugins/`:
- Custom hooks and operators for airline data processing
- Extended Airflow functionality for web scraping at scale
- Integration with Snowflake and S3 services for big data workflows

---

## 📦 Key Dependencies

- `pandas==1.5.3` - Data processing and analysis
- `requests` - HTTP library for web scraping
- `beautifulsoup4` - HTML parsing for airline review extraction
- `apache-airflow-providers-snowflake` - Snowflake integration
- `snowflake-connector-python` - Direct Snowflake connectivity
- `boto3==1.35.0` - AWS S3 operations
- `apache-airflow-providers-amazon` - AWS services integration

## 🚀 Key Features

- **Comprehensive Coverage**: Extracts reviews from 500+ airlines on Skytrax
- **Scalable Architecture**: Handles 100,000+ reviews with robust error handling
- **Airline Identification**: Each review tagged with airline name for analysis
- **Intelligent Discovery**: Automatically finds and processes all available airlines
- **Production Ready**: Containerized with Docker for reliable deployment

---
