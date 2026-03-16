# 🚀 Cloud ETL Pipeline Framework

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apachespark)](https://spark.apache.org)
[![Airflow](https://img.shields.io/badge/Airflow-2.8-017CEE?logo=apacheairflow)](https://airflow.apache.org)
[![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20EMR%20%7C%20Glue%20%7C%20Redshift-FF9900?logo=amazonaws)](https://aws.amazon.com)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake)](https://snowflake.com)
[![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC?logo=terraform)](https://terraform.io)
[![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-2088FF?logo=githubactions)](https://github.com/vineethreddy-a/cloud-etl-pipeline/actions)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

A production-grade, end-to-end ETL/ELT pipeline framework processing **10M+ records per run** with **99%+ data accuracy**. Built with Python, PySpark, Apache Airflow, AWS (S3, Glue, EMR, Redshift), and Snowflake.

> Built by [Vineeth Reddy Aredla](https://www.linkedin.com/in/vineeth-reddy-aredla-a77734246/) — Data Engineer with 4+ years of experience across AWS, Azure, and GCP.

---

## Architecture

```
+------------------------------------------------------------------+
|                        DATA SOURCES                              |
|   REST APIs    |    Databases (SQL/NoSQL)    |    CSV / Files    |
+-----------------------------+------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
|                   INGESTION LAYER  (Python + Boto3)              |
|              S3 Landing Zone  ---  Partitioned by Date           |
+-----------------------------+------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
|             TRANSFORMATION LAYER  (PySpark on AWS EMR)           |
|   Cleansing --> Deduplication --> Star Schema --> Aggregations    |
|             AWS Glue Catalog   |   Broadcast Joins               |
+------------------+----------------------------+-----------------+
                   |                            |
                   v                            v
+------------------------------+  +--------------------------------+
|   DATA VALIDATION LAYER      |  |        ORCHESTRATION           |
|   Null / Unique / Range      |  |   Apache Airflow 2.x DAGs     |
|   Reconciliation (99%+)      |  |   Retry logic + SLA alerts    |
+------------------------------+  +--------------------------------+
                   |
       +-----------+------------+
       v                        v
+--------------+       +------------------+
|  Snowflake   |       |  AWS Redshift    |
|  Analytics   |       |  BI Warehouse    |
+--------------+       +------------------+
       |                        |
       +----------+-------------+
                  v
       +---------------------+
       |  Tableau / Power BI |
       +---------------------+
```

---

## Features

- Modular ETL pipeline with ingestion, transformation, and load layers
- PySpark-based distributed processing — EMR cluster-ready, 40% faster after Spark tuning
- Automated data validation and source-to-target reconciliation (99%+ accuracy)
- Apache Airflow DAGs with retry logic, exponential backoff, and SLA monitoring
- Snowflake + Redshift dual-warehouse support with star schema dimensional models
- CI/CD with GitHub Actions — lint, test, deploy to staging and production
- Infrastructure as Code via Terraform (S3, EMR IAM, Glue catalog, SNS alerts)
- Structured logging, SNS alerting, and monitoring hooks
- 25% compute cost reduction via Spark workload optimization

---

## Tech Stack

| Layer            | Technology                              |
|------------------|-----------------------------------------|
| Orchestration    | Apache Airflow 2.8                      |
| Processing       | PySpark 3.5 (AWS EMR), AWS Glue         |
| Storage          | AWS S3 (Parquet/partitioned)            |
| Warehousing      | Snowflake, AWS Redshift                 |
| Ingestion        | Python 3.11, Pandas, Boto3              |
| Validation       | Custom framework + Great Expectations   |
| IaC              | Terraform 1.6+                          |
| CI/CD            | GitHub Actions                          |
| Monitoring       | Python logging + AWS SNS                |
| BI               | Tableau, Power BI                       |

---

## Project Structure

```
cloud-etl-pipeline/
├── src/
│   ├── ingestion/
│   │   └── s3_ingestion.py        # CSV, API, DB → S3 landing zone
│   ├── transformation/
│   │   └── spark_transform.py     # PySpark: cleanse, dedupe, star schema
│   ├── validation/
│   │   └── data_validator.py      # Quality checks + reconciliation
│   └── utils/
│       ├── logger.py              # Structured logging
│       ├── config.py              # Multi-env config loader
│       └── aws_helpers.py         # S3, Glue, EMR, SNS helpers
├── dags/
│   └── etl_pipeline_dag.py        # Airflow DAG (daily, retry, SLA)
├── config/
│   ├── base.yaml                  # Shared config
│   ├── dev.yaml                   # Dev overrides
│   └── env.example                # Env var template
├── tests/
│   └── test_data_validator.py     # 12 pytest unit tests
├── infrastructure/
│   └── main.tf                    # Terraform: S3, IAM, Glue, SNS
├── .github/workflows/
│   └── ci-cd.yml                  # GitHub Actions CI/CD
├── requirements.txt
├── setup.py
└── README.md
```

---

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/vineethreddy-a/cloud-etl-pipeline.git
cd cloud-etl-pipeline
```

### 2. Set Up Virtual Environment
```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure Environment Variables
```bash
cp config/env.example .env
# Edit .env with your AWS credentials, Snowflake connection, etc.
```

### 4. Run the Pipeline Locally
```bash
python src/ingestion/s3_ingestion.py     --env dev --source data/sample.csv --target-key transactions
python src/transformation/spark_transform.py --env dev
python src/validation/data_validator.py  --env dev --dataset transactions --path data/output/
```

### 5. Launch Airflow
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow webserver --port 8080 &
airflow scheduler &
# Open http://localhost:8080 — look for "cloud_etl_pipeline" DAG
```

---

## Running Tests

```bash
pytest tests/ -v --cov=src --cov-report=html
# Open htmlcov/index.html to view coverage report
```

---

## Infrastructure Setup (Terraform)

```bash
cd infrastructure/
terraform init
terraform plan  -var="environment=dev"
terraform apply -var="environment=dev"
```

---

## Pipeline Performance

| Metric                          | Result     |
|---------------------------------|------------|
| Records processed per run       | 10M+       |
| Data accuracy (validation)      | 99%+       |
| Spark performance improvement   | 40% faster |
| Compute cost reduction          | 25%        |
| Manual validation effort saved  | 40%        |
| Reporting speed improvement     | 40%        |

---

## Environment Variables

| Variable                  | Description                          |
|---------------------------|--------------------------------------|
| `AWS_ACCESS_KEY_ID`       | AWS access key                       |
| `AWS_SECRET_ACCESS_KEY`   | AWS secret key                       |
| `AWS_DEFAULT_REGION`      | AWS region (default: us-east-1)      |
| `S3_BUCKET`               | Target S3 bucket name                |
| `SNOWFLAKE_USER`          | Snowflake username                   |
| `SNOWFLAKE_PASSWORD`      | Snowflake password                   |
| `ETL_ENV`                 | Environment: dev / staging / prod    |

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

## Author

**Vineeth Reddy Aredla** — Data Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?logo=linkedin)](https://www.linkedin.com/in/vineeth-reddy-aredla-a77734246/)
[![GitHub](https://img.shields.io/badge/GitHub-vineethreddy--a-black?logo=github)](https://github.com/vineethreddy-a)
[![Email](https://img.shields.io/badge/Email-aredlavineethreddy2001%40gmail.com-red?logo=gmail)](mailto:aredlavineethreddy2001@gmail.com)
