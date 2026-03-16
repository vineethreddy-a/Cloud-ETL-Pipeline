"""
Apache Airflow DAG: End-to-End ETL Pipeline
Orchestrates ingestion → transformation → validation → load
with retry logic, alerting, and SLA monitoring.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# ─── Default Args ────────────────────────────────────────────────────────────

default_args = {
    "owner": "vineeth.aredla",
    "depends_on_past": False,
    "email": ["aredlavineethreddy2001@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=3),
}

ENV = Variable.get("ETL_ENV", default_var="dev")

# ─── DAG Definition ──────────────────────────────────────────────────────────

with DAG(
    dag_id="cloud_etl_pipeline",
    default_args=default_args,
    description="End-to-end ETL: S3 ingestion → PySpark transform → validation → Snowflake/Redshift",
    schedule_interval="0 6 * * *",       # Daily at 06:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "data-engineering", "production"],
) as dag:

    # ── Start ─────────────────────────────────────────────────────────────────

    start = DummyOperator(task_id="start")

    # ── Ingestion ─────────────────────────────────────────────────────────────

    ingest_transactions = BashOperator(
        task_id="ingest_transactions",
        bash_command=(
            f"python /opt/airflow/src/ingestion/s3_ingestion.py "
            f"--env {ENV} --source-type db "
            f"--source transactions --target-key transactions"
        ),
    )

    ingest_customers = BashOperator(
        task_id="ingest_customers",
        bash_command=(
            f"python /opt/airflow/src/ingestion/s3_ingestion.py "
            f"--env {ENV} --source-type api "
            f"--source ${{CUSTOMER_API_ENDPOINT}} --target-key customers"
        ),
    )

    ingest_products = BashOperator(
        task_id="ingest_products",
        bash_command=(
            f"python /opt/airflow/src/ingestion/s3_ingestion.py "
            f"--env {ENV} --source-type csv "
            f"--source s3://raw-data/products/latest.csv --target-key products"
        ),
    )

    # ── Transformation ────────────────────────────────────────────────────────

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            f"spark-submit "
            f"--master yarn "
            f"--deploy-mode cluster "
            f"--num-executors 10 "
            f"--executor-memory 8g "
            f"--executor-cores 4 "
            f"--conf spark.sql.shuffle.partitions=400 "
            f"/opt/airflow/src/transformation/spark_transform.py --env {ENV}"
        ),
        execution_timeout=timedelta(hours=2),
    )

    # ── Validation ────────────────────────────────────────────────────────────

    def validate_data(**context):
        import subprocess
        import sys

        result = subprocess.run(
            [
                sys.executable,
                "/opt/airflow/src/validation/data_validator.py",
                "--env", ENV,
                "--dataset", "fact_transactions",
                "--path", f"s3://data-lake/curated/fact_transactions/",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise ValueError(f"Data validation failed:\n{result.stdout}\n{result.stderr}")

        context["task_instance"].xcom_push(key="validation_output", value=result.stdout)
        return "load_snowflake"

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        provide_context=True,
    )

    # ── Load ──────────────────────────────────────────────────────────────────

    load_snowflake = BashOperator(
        task_id="load_snowflake",
        bash_command=(
            f"python /opt/airflow/src/ingestion/s3_ingestion.py "
            f"--env {ENV} --target-key snowflake_load"
        ),
    )

    load_redshift = BashOperator(
        task_id="load_redshift",
        bash_command=(
            f"python /opt/airflow/src/ingestion/s3_ingestion.py "
            f"--env {ENV} --target-key redshift_load"
        ),
    )

    # ── End ───────────────────────────────────────────────────────────────────

    end = DummyOperator(task_id="end")

    # ── Task Dependencies ─────────────────────────────────────────────────────

    start >> [ingest_transactions, ingest_customers, ingest_products]
    [ingest_transactions, ingest_customers, ingest_products] >> spark_transform
    spark_transform >> validate
    validate >> [load_snowflake, load_redshift]
    [load_snowflake, load_redshift] >> end
