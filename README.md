# British Airways Extract-Load

This project implements a modern Extract-Load pipeline for British Airways, designed to process and analyze customer review data from [Airline Quality](https://www.airlinequality.com/airline-reviews/british-airways/). It leverages **Apache Airflow**, **Snowflake**, **AWS S3**, and **Docker** to load data into Snowflake before transformation using dbt.

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
 [AirlineQuality.com](https://www.airlinequality.com/airline-reviews/british-airways/)
- **Data Processing**: Python 3.12 with Pandas
- **Workflow Orchestration**: Apache Airflow, Astronomer
- **Data Warehouse**: Snowflake
- **Data Lake**: AWS S3 for staging
- **Containerization**: Docker

---

## 🧱 Data Architecture

### 1. Data Source
The project processes customer review data scraped from, which contains detailed information about customer flight experiences.

### 2. Data Processing Pipeline
1. **Data Crawling**
   - Crawl customer reviews from AirlineQuality.com
   - Store raw data as `raw_data.csv`

2. **Data Cleaning & Transformation**
   - Process and clean the raw data (see [British Airways Data Cleaning Repository](https://github.com/DucLe-2005/british_airways_data_cleaning))
   - Standardize formats and handle missing values
   - Generate cleaned dataset

3. **Staging in S3**
   - Upload cleaned data to AWS S3 bucket (`upload_cleaned_data_to_s3`)
   - Store in staging area for Snowflake ingestion
   - Maintain data versioning and audit trail
<img width="1560" height="540" alt="BritishAirways (1)" src="https://github.com/user-attachments/assets/e39ce225-c54a-4622-8b23-ada013f90003" />

4. **Snowflake Loading**
   - Use Snowflake COPY operator to load data from S3
   - Transform and load into target tables
   - Implement incremental loading strategy

### 3. Data Quality Framework
- Data validation checks
- Error handling and logging
- Pipeline monitoring and alerting
- Snowflake data quality monitoring

---

## 🧩 Project Components

### 📊 Airflow DAGs
Located in `airflow/dags/`:
- DAG definitions for data processing workflows
- Task scheduling and dependency management
- Error handling and retry logic
- Snowflake data loading and transformation tasks

### 🛠 Custom Tasks
Located in `airflow/tasks/`:
- Data processing and transformation logic
- S3 upload operations
- Snowflake data loading and unloading operations
- Custom operators for specific business requirements
- Utility functions for data handling

### 🔌 Airflow Plugins
Located in `airflow/plugins/`:
- Custom hooks and operators
- Extended Airflow functionality
- Integration with Snowflake and S3 services

---

## 📦 Key Dependencies

- `pandas==1.5.3`
- `apache-airflow-providers-snowflake`
- `snowflake-connector-python`
- `boto3==1.35.0`
- `apache-airflow-providers-amazon`

---
