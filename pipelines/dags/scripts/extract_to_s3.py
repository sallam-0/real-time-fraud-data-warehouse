"""
Extract MSSQL Source Tables to AWS S3 as Parquet
================================================

Connects to MSSQL (BankingSystem), reads each dimension source table,
adds ETL metadata columns (_BATCH_ID, _SOURCE_SYSTEM, _LOAD_TS, _ROW_HASH),
and writes the result as Parquet files to S3.

Output path:
    s3://{bucket}/batch/raw/{table_name}/batch_{batch_id}/data.parquet

Author: Data Engineering Team
"""

import logging
import os
from datetime import datetime, timezone
from typing import List, Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc

logger = logging.getLogger(__name__)

# ── Source table definitions ─────────────────────────────────────────────

from scripts.sql_queries_extract import SOURCE_TABLES



def extract_table_to_s3(
    mssql_conn_str: str,
    s3_bucket: str,
    s3_prefix: str,
    table_cfg: dict,
    batch_id: str,
    s3_client=None,
) -> int:
    """
    Extract one MSSQL table to S3 as Parquet.

    Returns the number of rows extracted.
    """
    import time

    table_name = table_cfg["table"]
    s3_folder = table_cfg["s3_folder"]
    query = table_cfg["query"]

    logger.info(f"Extracting {table_name} from MSSQL...")

    # Step 1: Fetch from MSSQL using cursor (much faster than pd.read_sql)
    t0 = time.time()
    conn = pyodbc.connect(mssql_conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
    finally:
        conn.close()
    t1 = time.time()
    logger.info(f"  [{table_name}] SQL fetch: {t1-t0:.1f}s, {len(rows)} rows")

    if not rows:
        logger.warning(f"Table {table_name} returned 0 rows, skipping upload.")
        return 0

    # Step 2: Build Arrow table directly — skip pandas entirely
    t2 = time.time()
    # Transpose rows into columns for Arrow
    col_arrays = []
    for i, col_name in enumerate(columns):
        col_arrays.append(pa.array([row[i] for row in rows], from_pandas=True))

    # Add metadata columns
    n = len(rows)
    now_ts = datetime.now(timezone.utc).isoformat()
    col_arrays.append(pa.array([batch_id] * n))
    col_arrays.append(pa.array(["MSSQL_BankingSystem"] * n))
    col_arrays.append(pa.array([now_ts] * n))

    all_columns = columns + ["_BATCH_ID", "_SOURCE_SYSTEM", "_LOAD_TS"]
    table = pa.table(dict(zip(all_columns, col_arrays)))

    # Cast all columns to string — raw Snowflake tables are all VARCHAR
    string_cols = []
    for col in table.column_names:
        string_cols.append(table.column(col).cast(pa.string()))
    table = pa.table(dict(zip(table.column_names, string_cols)))

    t3 = time.time()
    logger.info(f"  [{table_name}] Arrow build + cast: {t3-t2:.1f}s")

    # Step 3 & 4: Write + upload in chunks (small files upload reliably, large ones hang)
    import tempfile
    import os as _os
    from botocore.config import Config

    CHUNK_SIZE = 2000  # rows per file

    if s3_client is None:
        s3_client = boto3.client(
            "s3",
            config=Config(
                connect_timeout=10,
                read_timeout=60,
                retries={"max_attempts": 3},
            ),
        )

    num_chunks = (table.num_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
    for chunk_idx in range(num_chunks):
        start = chunk_idx * CHUNK_SIZE
        end = min(start + CHUNK_SIZE, table.num_rows)
        chunk = table.slice(start, end - start)

        # Write chunk to temp file
        tmp_path = tempfile.mktemp(suffix=".parquet")
        pq.write_table(chunk, tmp_path, compression="snappy")
        chunk_size = _os.path.getsize(tmp_path)

        # Upload chunk
        s3_key = f"{s3_prefix}/{s3_folder}/batch_{batch_id}/part_{chunk_idx:04d}.parquet"
        try:
            s3_client.upload_file(tmp_path, s3_bucket, s3_key)
            t_chunk = time.time()
            logger.info(
                f"  [{table_name}] chunk {chunk_idx+1}/{num_chunks}: "
                f"{end-start} rows, {chunk_size/1024:.0f}KB, uploaded in {t_chunk-t3:.1f}s"
            )
        finally:
            _os.unlink(tmp_path)
        t3 = time.time()  # reset for next chunk timing

    t5 = time.time()
    logger.info(f"  [{table_name}] TOTAL: {t5-t0:.1f}s — {len(rows)} rows in {num_chunks} chunks")
    return len(rows)



def extract_all_tables(
    mssql_conn_str: str,
    s3_bucket: str,
    s3_prefix: str = "batch/raw",
    batch_id: Optional[str] = None,
    tables: Optional[List[str]] = None,
) -> dict:
    """
    Extract all (or selected) MSSQL source tables to S3.

    Args:
        mssql_conn_str: ODBC connection string for MSSQL
        s3_bucket: Target S3 bucket name
        s3_prefix: S3 key prefix (default: batch/raw)
        batch_id: Unique batch identifier (auto-generated if None)
        tables: Optional list of table names to extract (None = all)

    Returns:
        dict with table names as keys and row counts as values
    """
    if batch_id is None:
        batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    s3_client = boto3.client("s3")
    results = {}

    for table_cfg in SOURCE_TABLES:
        if tables and table_cfg["table"] not in tables:
            continue

        try:
            row_count = extract_table_to_s3(
                mssql_conn_str=mssql_conn_str,
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix,
                table_cfg=table_cfg,
                batch_id=batch_id,
                s3_client=s3_client,
            )
            results[table_cfg["table"]] = row_count
        except Exception as e:
            logger.error(f"Failed to extract {table_cfg['table']}: {e}")
            results[table_cfg["table"]] = -1

    logger.info(f"Extraction complete. Results: {results}")
    return results


if __name__ == "__main__":
    # For manual testing
    import sys
    logging.basicConfig(level=logging.INFO)

    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=BankingSystem;"
        "UID=sa;PWD=;"
        "TrustServerCertificate=yes;"
    )
    result = extract_all_tables(
        mssql_conn_str=conn_str,
        s3_bucket="aws.s3-sallameu",
    )
    print(result)
