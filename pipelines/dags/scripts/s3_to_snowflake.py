"""
S3 to Snowflake Loader via COPY INTO
=====================================

Loads Parquet files from S3 external stages into Snowflake staging tables
using COPY INTO. Handles both:
  - Batch data (MSSQL extracts → RAW_* tables)
  - Real-time data (Flink output → RAW_REALTIME_* tables)

Snowflake's COPY INTO tracks loaded files automatically, so re-runs
skip already-loaded files (idempotent by default).

Author: Data Engineering Team
"""

import logging
from typing import Optional

import snowflake.connector

logger = logging.getLogger(__name__)


# ── Table-to-stage mapping ───────────────────────────────────────────────

from scripts.snowflake_schemas import (
    BATCH_COPY_CONFIGS,
    REALTIME_COPY_CONFIGS,
    BATCH_RAW_TABLE_DDL,
)


def ensure_batch_tables(conn):
    """Create or replace all batch RAW tables so columns match extract queries."""
    cursor = conn.cursor()
    for table_name, columns in BATCH_RAW_TABLE_DDL.items():
        col_defs = ",\n    ".join(f"{c} VARCHAR" for c in columns)
        ddl = f"""
            CREATE OR REPLACE TABLE {table_name} (
                {col_defs},
                _BATCH_ID       VARCHAR,
                _SOURCE_SYSTEM  VARCHAR,
                _LOAD_TS        VARCHAR
            )
        """
        try:
            cursor.execute(ddl)
            logger.info(f"Created table {table_name} with {len(columns)} columns")
        except Exception as e:
            logger.warning(f"DDL for {table_name}: {e}")
    cursor.close()


def _get_snowflake_conn(sf_cfg: dict):
    """Create a Snowflake connection from config dict."""
    return snowflake.connector.connect(
        account=sf_cfg["account"],
        user=sf_cfg["user"],
        password=sf_cfg["password"],
        database=sf_cfg["database"],
        schema=sf_cfg.get("schema", "STAGING"),
        warehouse=sf_cfg["warehouse"],
        role=sf_cfg.get("role", "ACCOUNTADMIN"),
    )


def copy_into_table(
    conn,
    target_table: str,
    stage: str,
    file_format: str = "STAGING.PARQUET_FORMAT",
    pattern: str = ".*\\.parquet",
) -> dict:
    """
    Execute COPY INTO for a single table/stage combination.

    Returns dict with rows_loaded and status.
    """
    sql = f"""
        COPY INTO {target_table}
        FROM {stage}
        FILE_FORMAT = {file_format}
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PATTERN = '{pattern}'
        ON_ERROR = 'CONTINUE'
    """

    logger.info(f"Running COPY INTO {target_table} FROM {stage}...")

    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        results = cursor.fetchall()

        total_rows = 0
        total_errors = 0
        files_loaded = 0

        for idx, row in enumerate(results):
            # COPY INTO returns: (file, status, rows_parsed, rows_loaded,
            #                     error_limit, errors_seen, first_error, ...)
            if len(row) >= 4:
                status = row[1] if len(row) > 1 else "UNKNOWN"
                rows_loaded = row[3] if len(row) > 3 else 0
                errors_seen = row[5] if len(row) > 5 else 0
                first_error = row[6] if len(row) > 6 else ""
                total_rows += rows_loaded or 0
                total_errors += errors_seen or 0
                files_loaded += 1
                # Log first error from first file for debugging
                if idx == 0 and errors_seen and first_error:
                    logger.warning(f"  First error in {target_table}: {first_error}")

        logger.info(
            f"COPY INTO {target_table}: "
            f"{files_loaded} files, {total_rows} rows loaded, "
            f"{total_errors} errors"
        )

        return {
            "table": target_table,
            "files_loaded": files_loaded,
            "rows_loaded": total_rows,
            "errors": total_errors,
            "status": "SUCCESS" if total_errors == 0 else "PARTIAL",
        }

    except Exception as e:
        logger.error(f"COPY INTO {target_table} failed: {e}")
        return {
            "table": target_table,
            "files_loaded": 0,
            "rows_loaded": 0,
            "errors": -1,
            "status": "FAILED",
            "error_message": str(e),
        }
    finally:
        cursor.close()


def load_batch_data(sf_cfg: dict) -> list:
    """
    Load all batch MSSQL extracts from S3 into Snowflake staging tables.

    Returns list of result dicts.
    """
    conn = _get_snowflake_conn(sf_cfg)
    results = []

    try:
        ensure_batch_tables(conn)
        for cfg in BATCH_COPY_CONFIGS:
            result = copy_into_table(
                conn=conn,
                target_table=cfg["target_table"],
                stage=cfg["stage"],
            )
            results.append(result)
    finally:
        conn.close()

    return results


def load_realtime_data(sf_cfg: dict) -> list:
    """
    Load real-time Flink output from S3 into Snowflake staging tables.

    Returns list of result dicts.
    """
    conn = _get_snowflake_conn(sf_cfg)
    results = []

    try:
        for cfg in REALTIME_COPY_CONFIGS:
            result = copy_into_table(
                conn=conn,
                target_table=cfg["target_table"],
                stage=cfg["stage"],
            )
            results.append(result)
    finally:
        conn.close()

    return results


def load_all_data(sf_cfg: dict) -> dict:
    """
    Load both batch and real-time data from S3 into Snowflake.

    Returns dict with batch_results and realtime_results.
    """
    return {
        "batch_results": load_batch_data(sf_cfg),
        "realtime_results": load_realtime_data(sf_cfg),
    }


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    sf_cfg = {
        "account": "AGREMWF-RQ59577",
        "user": "SALLAM",
        "password": "<YOUR_PASSWORD>",
        "database": "FRAUD_DWH",
        "schema": "STAGING",
        "warehouse": "COMPUTE_WH",
        "role": "ACCOUNTADMIN",
    }

    results = load_all_data(sf_cfg)
    for category, data in results.items():
        print(f"\n{category}:")
        for r in data:
            print(f"  {r['table']}: {r['rows_loaded']} rows ({r['status']})")
