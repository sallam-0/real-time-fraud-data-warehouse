"""
Fraud DWH Airflow DAG — S3-First ELT Pipeline
==============================================

Orchestrates the full ELT pipeline:
  1. Extract MSSQL source tables → S3 (Parquet)
  2. COPY INTO from S3 → Snowflake STAGING (batch + real-time)
  3. dbt staging models (views)
  4. dbt dimension models
  5. dbt fact models
  6. dbt tests

Schedule: Every 6 hours (batch dimensions + snapshot facts only)

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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ── Helper functions (called by PythonOperators) ─────────────────────────

def extract_mssql_to_s3(**context):
    """Extract all MSSQL source tables to S3 as Parquet."""
    import os
    from extract_to_s3 import extract_all_tables
    from db_config import get_mssql_conn_str, _get

    mssql_conn_str = get_mssql_conn_str()
    s3_bucket = _get("S3_BUCKET", "aws.s3-sallameu")
    batch_id = context["ds_nodash"] + "_" + context["run_id"][:8]

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
        mssql_conn_str=mssql_conn_str,
        s3_bucket=s3_bucket,
        batch_id=batch_id,
    )

    # Push batch_id to XCom for downstream tasks
    context["ti"].xcom_push(key="batch_id", value=batch_id)
    context["ti"].xcom_push(key="extract_results", value=results)
    return results



def load_batch_s3_to_snowflake(**context):
    """COPY INTO Snowflake staging tables from S3 (batch data)."""
    from s3_to_snowflake import load_batch_data
    from db_config import get_snowflake_cfg

    sf_cfg = get_snowflake_cfg()
    results = load_batch_data(sf_cfg)
    context["ti"].xcom_push(key="batch_load_results", value=results)
    return results


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
    dag_id="fraud_dwh_s3_elt_pipeline",
    default_args=default_args,
    description="S3-first ELT pipeline: MSSQL→S3, Flink→S3, S3→Snowflake, dbt transforms",
    schedule_interval="0 */6 * * *",  # Every 6 hours
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fraud-dwh", "s3", "snowflake", "dbt"],
) as dag:

    # ── Task 1: Extract MSSQL → S3 ──────────────────────────────────────
    extract_mssql = PythonOperator(
        task_id="extract_mssql_to_s3",
        python_callable=extract_mssql_to_s3,
        provide_context=True,
    )

    # ── Task 2a: S3 → Snowflake (batch data) ────────────────────────────
    load_batch = PythonOperator(
        task_id="s3_to_snowflake_batch",
        python_callable=load_batch_s3_to_snowflake,
        provide_context=True,
    )


    # ── Task 3: dbt staging models ──────────────────────────────────────
    batch_id_template = "{{ ti.xcom_pull(task_ids='extract_mssql_to_s3', key='batch_id') or 'MANUAL_RUN' }}"

    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps --profiles-dir . && "
            f"dbt run --select staging --profiles-dir . "
            f"--vars '{{\"batch_id\": \"{batch_id_template}\"}}'"
        ),
        env=dbt_env,
    )

    # ── Task 4: dbt dimension models ────────────────────────────────────
    dbt_dimensions = BashOperator(
        task_id="dbt_run_dimensions",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select dimensions --full-refresh --profiles-dir . "
            f"--vars '{{\"batch_id\": \"{batch_id_template}\"}}'"
        ),
        env=dbt_env,
    )

    # ── Task 5: dbt fact models ─────────────────────────────────────────
    dbt_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select facts --full-refresh --profiles-dir . "
            f"--vars '{{\"batch_id\": \"{batch_id_template}\"}}'"
        ),  
        env=dbt_env,
    )

    # ── Task 6: dbt tests ───────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir ."
        ),
        env=dbt_env,
    )

    # ── Task 7: Completion notification ─────────────────────────────────
    notify = BashOperator(
        task_id="notify_complete",
        bash_command='echo "Fraud DWH ELT pipeline completed at $(date)"',
    )

    # ── Task dependencies ───────────────────────────────────────────────
    #
    #   extract_mssql → load_batch → dbt_staging → dbt_dims → dbt_facts → dbt_test → notify
    #
    extract_mssql >> load_batch >> dbt_staging
    dbt_staging >> dbt_dimensions >> dbt_facts >> dbt_test >> notify
