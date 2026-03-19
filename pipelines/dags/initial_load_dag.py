"""
Fraud DWH — Initial Batch Load DAG
====================================

One-time (manual trigger) DAG that performs the full initial load:
  1. Extract ALL MSSQL source tables → S3 (Parquet)
  2. COPY INTO from S3 → Snowflake STAGING
  3. dbt deps (install packages)
  4. dbt run staging models (views)
  5. dbt run dimension models (--full-refresh)
  6. dbt run fact models (--full-refresh)
  7. dbt test (validate data quality)

This DAG is NOT scheduled — trigger it manually from the Airflow UI.

Author: Data Engineering Team
"""

from datetime import datetime, timedelta
import os
import sys
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# Add the scripts directory to the path
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(DAGS_DIR, "scripts")
sys.path.insert(0, SCRIPTS_DIR)

# dbt project path (mounted into Airflow container)
DBT_PROJECT_DIR = os.environ.get(
    "DBT_PROJECT_DIR", "/opt/airflow/dbt_fraud_dwh"
)

logger = logging.getLogger(__name__)


# ── Default DAG args ─────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=4),
}


# Configs moved to scripts/db_config.py


def extract_all_to_s3(**context):
    """Extract ALL MSSQL source tables to S3 as Parquet."""
    import os
    from extract_to_s3 import extract_all_tables
    from db_config import get_mssql_conn_str, _get

    batch_id = "INITIAL_LOAD_" + datetime.now().strftime("%Y%m%d_%H%M%S")

    # Inject AWS credentials from Docker secrets into env so boto3 can find them
    aws_key = _get("AWS_ACCESS_KEY_ID")
    aws_secret = _get("AWS_SECRET_ACCESS_KEY")
    aws_region = _get("AWS_DEFAULT_REGION", "eu-central-1")
    if aws_key:
        os.environ["AWS_ACCESS_KEY_ID"] = aws_key
    if aws_secret:
        os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret
    if aws_region:
        os.environ["AWS_DEFAULT_REGION"] = aws_region

    results = extract_all_tables(
        mssql_conn_str=get_mssql_conn_str(),
        s3_bucket=_get("S3_BUCKET", "aws.s3-sallameu"),
        batch_id=batch_id,
    )

    context["ti"].xcom_push(key="batch_id", value=batch_id)
    context["ti"].xcom_push(key="extract_results", value=results)

    total_rows = sum(v for v in results.values() if v > 0)
    failed = [k for k, v in results.items() if v < 0]
    logger.info(f"Initial extraction complete: {total_rows} total rows across {len(results)} tables")
    if failed:
        logger.warning(f"Failed tables: {failed}")

    return results


def load_all_to_snowflake(**context):
    """COPY INTO Snowflake staging tables from S3 (all batch data)."""
    from s3_to_snowflake import load_batch_data
    from db_config import get_snowflake_cfg

    sf_cfg = get_snowflake_cfg()
    results = load_batch_data(sf_cfg)
    context["ti"].xcom_push(key="load_results", value=results)
    return results


# ── Prepare DBT Environment Variables ────────────────────────────────────
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

# ── DAG Definition ───────────────────────────────────────────────────────

with DAG(
    dag_id="fraud_dwh_initial_load",
    default_args=default_args,
    description="One-time initial batch load: MSSQL → S3 → Snowflake → dbt full-refresh",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fraud-dwh", "initial-load", "batch", "full-refresh"],
) as dag:

    # ── Task 1: Extract ALL source tables → S3 ──────────────────────────
    extract = PythonOperator(
        task_id="extract_mssql_to_s3",
        python_callable=extract_all_to_s3,
        provide_context=True,
    )

    # ── Task 2: S3 → Snowflake STAGING ──────────────────────────────────
    load = PythonOperator(
        task_id="s3_to_snowflake_staging",
        python_callable=load_all_to_snowflake,
        provide_context=True,
    )

    # ── Task 3: dbt deps ────────────────────────────────────────────────
    batch_id_template = "{{ ti.xcom_pull(task_ids='extract_mssql_to_s3', key='batch_id') or 'INITIAL_LOAD' }}"

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir .",
        env=dbt_env,
    )

    # ── Task 4: dbt staging views ───────────────────────────────────────
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select staging --profiles-dir . "
            f"--vars '{{\"batch_id\": \"{batch_id_template}\"}}'"
        ),
        env=dbt_env,
    )

    # ── Task 5: dbt dimensions (--full-refresh) ─────────────────────────
    dbt_dimensions = BashOperator(
        task_id="dbt_run_dimensions",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select dimensions --full-refresh --profiles-dir . "
            f"--vars '{{\"batch_id\": \"{batch_id_template}\"}}'"
        ),
        env=dbt_env,
    )

    # ── Task 6: dbt facts (--full-refresh) ──────────────────────────────
    dbt_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select facts --full-refresh --profiles-dir . "
            f"--vars '{{\"batch_id\": \"{batch_id_template}\"}}'"
        ),
        env=dbt_env,
    )

    # ── Task 7: dbt test ────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir . "
            f"--vars '{{\"batch_id\": \"{batch_id_template}\"}}'"
        ),
        env=dbt_env,
    )

    # ── Task 8: Completion log ──────────────────────────────────────────
    notify = BashOperator(
        task_id="initial_load_complete",
        bash_command=(
            'echo "========================================" && '
            'echo "INITIAL LOAD COMPLETED SUCCESSFULLY" && '
            'echo "Timestamp: $(date)" && '
            'echo "========================================"'
        ),
    )

    # ── Task dependencies ───────────────────────────────────────────────
    #
    #  extract → load → dbt_deps → dbt_staging → dbt_dims → dbt_facts → dbt_test → notify
    #
    extract >> load >> dbt_deps >> dbt_staging >> dbt_dimensions >> dbt_facts >> dbt_test >> notify
