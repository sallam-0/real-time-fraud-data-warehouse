"""
Real-Time S3 → Snowflake DAG (with S3 Sensor)
==============================================

This DAG runs every hour and:
  1. Senses new Parquet files in S3 from Flink real-time output
  2. Runs COPY INTO to load them into Snowflake staging tables
  3. Runs dbt staging + fact models for real-time data only

This is separate from the main batch DAG (fraud_dwh_s3_elt_pipeline)
which handles MSSQL extracts and dimension loads every 6 hours.

Schedule: Every hour
"""

from datetime import datetime, timedelta
import os
import sys
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# Add scripts dir to path
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(DAGS_DIR, "scripts"))

DBT_PROJECT_DIR = os.environ.get(
    "DBT_PROJECT_DIR", "/opt/airflow/dbt_fraud_dwh"
)

logger = logging.getLogger(__name__)


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def load_realtime_to_snowflake(**context):
    """COPY INTO for real-time Flink data from S3."""
    from s3_to_snowflake import load_realtime_data
    from db_config import get_snowflake_cfg

    sf_cfg = get_snowflake_cfg()
    results = load_realtime_data(sf_cfg)

    total_rows = sum(r.get("rows_loaded", 0) for r in results)
    logger.info(f"Real-time COPY INTO complete: {total_rows} total rows loaded")
    context["ti"].xcom_push(key="realtime_results", value=results)
    context["ti"].xcom_push(key="total_rows_loaded", value=total_rows)
    return results


with DAG(
    dag_id="realtime_s3_to_snowflake",
    default_args=default_args,
    description="Hourly: sense new Flink Parquet in S3, COPY INTO Snowflake, dbt transform",
    schedule_interval="* * * * *",
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fraud-dwh", "realtime", "s3", "snowflake"],
) as dag:

    # ── Task 1: Check for new Parquet files in S3 ─────────────────────
    def check_s3_for_files(**context):
        """Check if there are any Parquet files under the real-time prefix."""
        import boto3
        import os
        
        # Load AWS credentials securely from Docker secret
        aws_id, aws_secret = None, None
        if os.path.exists("/run/secrets/env_file"):
            with open("/run/secrets/env_file") as f:
                for line in f:
                    if line.startswith("AWS_ACCESS_KEY_ID="):
                        aws_id = line.split("=", 1)[1].strip()
                    elif line.startswith("AWS_SECRET_ACCESS_KEY="):
                        aws_secret = line.split("=", 1)[1].strip()
        
        s3 = boto3.client(
            "s3", 
            region_name="eu-central-1",
            aws_access_key_id=aws_id,
            aws_secret_access_key=aws_secret
        )
        response = s3.list_objects_v2(
            Bucket="aws.s3-sallameu",
            Prefix="real-time/transactions/",
            MaxKeys=1,
        )
        has_files = response.get("KeyCount", 0) > 0
        logger.info(f"S3 real-time prefix has files: {has_files}")
        return has_files

    check_files = PythonOperator(
        task_id="check_s3_for_files",
        python_callable=check_s3_for_files,
        provide_context=True,
    )

    # ── Task 2: COPY INTO Snowflake staging ──────────────────────────
    copy_into_snowflake = PythonOperator(
        task_id="copy_realtime_s3_to_snowflake",
        python_callable=load_realtime_to_snowflake,
        provide_context=True,
    )

    # ── Prepare DBT Environment Variables ────────────────────────────
    def _build_dbt_env():
        import os
        from db_config import get_snowflake_cfg
        sf_cfg = get_snowflake_cfg()
        env = os.environ.copy()
        env["DBT_SNOWFLAKE_ACCOUNT"] = sf_cfg.get("account", "")
        env["DBT_SNOWFLAKE_USER"] = sf_cfg.get("user", "")
        env["DBT_SNOWFLAKE_PASSWORD"] = sf_cfg.get("password", "")
        return env

    dbt_env = _build_dbt_env()

    # ── Task 3: dbt — run real-time staging models ───────────────────
    dbt_staging_realtime = BashOperator(
        task_id="dbt_run_staging_realtime",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps --profiles-dir . && "
            f"dbt run --select stg_realtime_transaction stg_realtime_fraud_detection "
            f"--profiles-dir ."
        ),
        env=dbt_env,
    )

    # ── Task 4: dbt — run fact models that use real-time data ────────
    dbt_facts_realtime = BashOperator(
        task_id="dbt_run_facts_realtime",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select fact_transaction fact_fraud_detection "
            f"--profiles-dir ."
        ),
        env=dbt_env,
    )

    # ── Task 5: dbt tests on real-time models ────────────────────────
    dbt_test_realtime = BashOperator(
        task_id="dbt_test_realtime",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --select fact_transaction fact_fraud_detection "
            f"--profiles-dir ."
        ),
        env=dbt_env,
    )

    notify = BashOperator(
        task_id="notify_complete",
        bash_command='echo "Real-time S3→Snowflake pipeline completed at $(date)"',
    )

    # ── Dependencies ─────────────────────────────────────────────────
    #
    #   sense_s3 → copy_into → dbt_staging → dbt_facts → dbt_test → notify
    #
    check_files >> copy_into_snowflake >> dbt_staging_realtime >> dbt_facts_realtime >> dbt_test_realtime >> notify
