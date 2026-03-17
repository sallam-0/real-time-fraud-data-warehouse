import sys
import logging
import time
import io
import uuid
from typing import List
from datetime import datetime, timezone
from pyflink.datastream.functions import MapFunction

try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

import json

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# S3 Sink — writes enriched+scored data as Parquet to AWS S3
# ---------------------------------------------------------------------------

# Transaction Parquet schema — enriched transaction data for FACT_TRANSACTION
TXN_PARQUET_COLUMNS = [
    "TransactionID", "AccountNumber", "CustomerID", "TransactionDate",
    "Amount", "Currency", "TransactionTypeID", "TransactionMethod",
    "Channel", "TransactionStatus",
    "City", "State", "Country", "Latitude", "Longitude", "IsInternational",
    "DeviceID", "DeviceType", "IPAddress",
    "CustomerSegment", "CustomerCity", "CustomerCountry",
    "AccountType", "AccountStatus", "CurrentBalance", "AvailableBalance",
    "transaction_hour", "transaction_day_of_week", "is_weekend",
    "is_night_transaction",
    "amount_to_balance_ratio", "amount_to_available_ratio",
    "is_high_value", "balance_utilization", "below_minimum_balance",
    "is_home_city", "is_home_country", "location_mismatch",
    "is_new_account", "is_dormant_account",
    "account_age_days", "days_since_last_activity",
    "customer_age_days", "customer_tenure_days", "is_new_customer",
    "is_active_account", "is_active_customer",
    "is_withdrawal", "is_transfer", "is_purchase", "is_deposit",
    "is_atm", "is_online", "is_mobile", "is_branch",
    "risk_score_base", "combined_risk_score",
    "transactions_last_1h", "transactions_last_24h",
    "amount_last_1h", "amount_last_24h", "avg_amount_24h",
    "processing_timestamp",
]

# Fraud Detection Parquet schema — ML scoring results for FACT_FRAUD_DETECTION
FRAUD_DET_PARQUET_COLUMNS = [
    "TransactionID", "fraud_score", "is_fraud_predicted",
    "confidence_score", "fraud_type", "risk_indication",
    "action_taken", "inference_latency_ms", "processing_timestamp",
]


def _parse_txn_datetime(raw_ts) -> datetime:
    """Parse Debezium epoch millis, epoch seconds, or ISO string to datetime."""
    if isinstance(raw_ts, (int, float)) and raw_ts > 1e12:
        return datetime.fromtimestamp(raw_ts / 1000, tz=timezone.utc)
    elif isinstance(raw_ts, (int, float)):
        return datetime.fromtimestamp(raw_ts, tz=timezone.utc)
    elif isinstance(raw_ts, str):
        try:
            return datetime.fromisoformat(raw_ts.replace('Z', '+00:00'))
        except ValueError:
            return datetime.now(timezone.utc)
    return datetime.now(timezone.utc)


class S3SinkMapFunction(MapFunction):
    """
    MapFunction that writes enriched + ML-scored transactions to AWS S3
    as Parquet files, partitioned by date and hour.

    Output paths:
      s3://{bucket}/{txn_prefix}/dt=YYYY-MM-DD/hr=HH/batch_{uuid}.parquet
      s3://{bucket}/{det_prefix}/dt=YYYY-MM-DD/hr=HH/batch_{uuid}.parquet

    PyFlink 1.18 does not support subclassing SinkFunction in Python,
    so this uses MapFunction and writes as a side-effect, returning
    the input string unchanged for downstream logging/debugging.
    """

    def __init__(self, s3_cfg: dict, ml_cfg: dict, batch_size: int = 100,
                 flush_interval_seconds: int = 60):
        self._s3_cfg = s3_cfg
        self._ml_cfg = ml_cfg
        self._batch_size = batch_size
        self._flush_interval = flush_interval_seconds
        self._s3_client = None
        self._txn_buffer: List[dict] = []
        self._det_buffer: List[dict] = []
        self._last_flush_time: float = 0
        self._total_flushed: int = 0

    def open(self, runtime_context):
        """Initialize S3 client when the task starts."""
        if not HAS_BOTO3:
            raise RuntimeError(
                "boto3 is not installed. Cannot write to S3 without it."
            )
        if not HAS_PYARROW:
            raise RuntimeError(
                "pyarrow is not installed. Cannot write Parquet without it."
            )

        self._s3_client = boto3.client(
            's3',
            region_name=self._s3_cfg.get('region', 'us-east-1'),
            aws_access_key_id=self._s3_cfg.get('access_key_id', ''),
            aws_secret_access_key=self._s3_cfg.get('secret_access_key', ''),
        )
        self._txn_buffer = []
        self._det_buffer = []
        self._last_flush_time = time.time()
        self._total_flushed = 0

        logger.info(
            f"S3 sink opened: bucket={self._s3_cfg['bucket']}, "
            f"batch_size={self._batch_size}, flush_interval={self._flush_interval}s"
        )

    def map(self, value: str) -> str:
        """Buffer record and flush to S3 when batch is full or timer expires."""
        try:
            record = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            logger.warning("S3Sink: skipping unparseable record")
            return value

        # Split record into transaction fields and fraud detection fields
        txn_record = {col: record.get(col) for col in TXN_PARQUET_COLUMNS}
        det_record = {col: record.get(col) for col in FRAUD_DET_PARQUET_COLUMNS}

        # Add model metadata to fraud detection record
        det_record["model_name"] = self._ml_cfg.get("model_name", "FraudGuard")
        det_record["model_version"] = self._ml_cfg.get("model_version", "v1.0.0")
        det_record["is_review_required"] = (
            record.get("fraud_score", 0) >= 0.6
        )

        self._txn_buffer.append(txn_record)
        self._det_buffer.append(det_record)

        # Flush on batch size or time interval
        elapsed = time.time() - self._last_flush_time
        if (len(self._txn_buffer) >= self._batch_size
                or elapsed >= self._flush_interval):
            self._flush()

        return value

    def close(self):
        """Flush remaining buffers on shutdown."""
        if self._txn_buffer or self._det_buffer:
            self._flush()
        logger.info(
            f"S3 sink closed. Total records flushed: {self._total_flushed}"
        )

    def _flush(self):
        """Write buffered records to S3 as Parquet files."""
        if not self._txn_buffer and not self._det_buffer:
            return

        now = datetime.now(timezone.utc)
        dt_partition = now.strftime("dt=%Y-%m-%d")
        hr_partition = now.strftime("hr=%H")
        batch_id = uuid.uuid4().hex[:12]

        bucket = self._s3_cfg['bucket']
        txn_prefix = self._s3_cfg.get(
            'transaction_prefix', 'real-time/transactions'
        )
        det_prefix = self._s3_cfg.get(
            'fraud_detection_prefix', 'real-time/fraud-detection'
        )

        # Flush transactions
        if self._txn_buffer:
            txn_key = (
                f"{txn_prefix}/{dt_partition}/{hr_partition}/"
                f"batch_{batch_id}.parquet"
            )
            self._write_parquet(bucket, txn_key, self._txn_buffer)
            count_txn = len(self._txn_buffer)
            self._txn_buffer.clear()
        else:
            count_txn = 0

        # Flush fraud detection
        if self._det_buffer:
            det_key = (
                f"{det_prefix}/{dt_partition}/{hr_partition}/"
                f"batch_{batch_id}.parquet"
            )
            self._write_parquet(bucket, det_key, self._det_buffer)
            count_det = len(self._det_buffer)
            self._det_buffer.clear()
        else:
            count_det = 0

        self._total_flushed += max(count_txn, count_det)
        self._last_flush_time = time.time()
        logger.info(
            f"Flushed to S3: {count_txn} transactions, "
            f"{count_det} fraud-detection records (batch {batch_id})"
        )

    def _write_parquet(self, bucket: str, key: str, records: List[dict]):
        """Serialize records to Parquet and upload to S3."""
        try:
            # Convert list of dicts to columnar format
            columns = {}
            for col_name in records[0].keys():
                values = [r.get(col_name) for r in records]
                # Convert all values to strings for safety (Snowflake
                # COPY INTO handles type casting via file format)
                columns[col_name] = [str(v) if v is not None else None
                                     for v in values]

            table = pa.table(columns)

            # Write Parquet to in-memory buffer
            buf = io.BytesIO()
            pq.write_table(table, buf, compression='snappy')
            buf.seek(0)

            # Upload to S3
            self._s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=buf.getvalue(),
                ContentType='application/octet-stream',
            )
            logger.debug(f"Wrote s3://{bucket}/{key} ({len(records)} rows)")

        except Exception as e:
            logger.error(
                f"S3 Parquet write failed for s3://{bucket}/{key}: {e}",
                exc_info=True,
            )
