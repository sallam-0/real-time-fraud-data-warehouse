"""
Flink Streaming Job: Transaction CDC Processing with ML Fraud Scoring
=====================================================================

This job:
1. Consumes Transaction CDC events from Kafka
2. Enriches with Customer and Account data from Redis (with MSSQL fallback)
3. Applies feature engineering for ML fraud detection
4. Runs the active ML model (Isolation Forest / supervised) to score transactions
5. Writes enriched + scored data to AWS S3 as Parquet files
   (partitioned by date/hour for downstream Snowflake COPY INTO)

"""

from typing import Optional, Dict, List

import json
import logging
import sys
import configparser
import os
import time
import hashlib
from datetime import datetime, timezone
import calendar
import uuid
import io

import numpy as np
import joblib

from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import MapFunction, FlatMapFunction
from pyflink.common.typeinfo import Types as FlinkTypes

import redis

try:
    import pyodbc
    HAS_PYODBC = True
except ImportError:
    HAS_PYODBC = False

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Redis / MSSQL helpers
# ---------------------------------------------------------------------------

# Add streaming directory to path so relative imports work
script_dir = os.path.dirname(os.path.abspath(__file__))
streaming_dir = os.path.dirname(script_dir)
if streaming_dir not in sys.path:
    sys.path.append(streaming_dir)

from src.infrastructure.redis_lookup import DimensionLookup

from src.domain.features import (
    RedisAggregator,
    engineer_features,
    parse_datetime,
    safe_float,
    safe_int,
    safe_bool,
    days_between,
    ML_FEATURE_COLUMNS,
)


class FraudModelScorer:
    """
    Loads a pre-trained scikit-learn model (Isolation Forest or supervised)
    and scores enriched transactions in the streaming pipeline.

    The model MUST be trained beforehand using train_fraud_model.py.
    If no model file exists at startup, scoring is disabled and all
    transactions receive safe default scores (fraud_score=0, action=Pass).

    The scorer is instantiated once per TaskManager in the MapFunction.open()
    lifecycle hook to avoid reloading the model for every record.
    """

    def __init__(self, model_path: str, threshold: float = 0.5,
                 model_name: str = "FraudGuard", model_version: str = "v1.0.0",
                 **kwargs):
        self._model_path = model_path
        self._threshold = threshold
        self._model_name = model_name
        self._model_version = model_version
        self._model = None
        self._is_isolation_forest = False
        self._records_seen = 0
        self._no_model_warned = False

    def load(self):
        """Load the serialized model from disk. Fails gracefully if absent."""
        if os.path.exists(self._model_path):
            try:
                self._model = joblib.load(self._model_path)
                model_type = type(self._model).__name__
                self._is_isolation_forest = "IsolationForest" in model_type
                logger.info(f"ML model loaded: {model_type} from {self._model_path}")
                logger.info(f"  Model: {self._model_name} {self._model_version}")
                logger.info(f"  Threshold: {self._threshold}")
                logger.info(f"  Type: {'unsupervised (Isolation Forest)' if self._is_isolation_forest else 'supervised'}")
                return
            except Exception as e:
                logger.error(f"Failed to load ML model: {e}", exc_info=True)
                self._model = None

        # No model on disk — scoring disabled
        logger.error(
            f"NO MODEL FOUND at {self._model_path} — scoring is DISABLED. "
            f"Run train_fraud_model.py first to train the model."
        )

    def _extract_feature_vector(self, features: dict) -> list:
        """Extract a numeric feature vector from an enriched feature dict."""
        vector = []
        for col in ML_FEATURE_COLUMNS:
            val = features.get(col, 0)
            try:
                vector.append(float(val) if val is not None else 0.0)
            except (ValueError, TypeError):
                vector.append(0.0)
        return vector

    def score(self, features: dict) -> dict:
        """
        Score a single enriched transaction dict.

        Returns a dict with ML output fields:
          fraud_score, is_fraud_predicted, confidence_score,
          fraud_type, risk_indication, action_taken, inference_latency_ms
        """
        self._records_seen += 1

        # --- No model loaded — return safe defaults ---
        if self._model is None:
            if not self._no_model_warned or self._records_seen % 1000 == 0:
                logger.warning(
                    f"No model loaded — returning safe defaults "
                    f"(record #{self._records_seen}). "
                    f"Run train_fraud_model.py to enable scoring."
                )
                self._no_model_warned = True
            return {
                "fraud_score": 0.0,
                "is_fraud_predicted": False,
                "confidence_score": 0.0,
                "fraud_type": None,
                "risk_indication": "Low",
                "action_taken": "Pass",
                "inference_latency_ms": 0,
            }

        # --- Normal scoring ---
        return self._score_with_model(features)

    def _score_with_model(self, features: dict) -> dict:
        """Score a transaction using the loaded ML model."""
        start_time = time.time()

        vector = self._extract_feature_vector(features)
        X = np.array([vector])

        try:
            if self._is_isolation_forest:
                raw_score = self._model.decision_function(X)[0]
                fraud_score = max(0.0, min(1.0, 0.5 - raw_score))
                prediction = self._model.predict(X)[0]
                is_fraud_predicted = prediction == -1 or fraud_score >= self._threshold
            else:
                if hasattr(self._model, 'predict_proba'):
                    probas = self._model.predict_proba(X)[0]
                    fraud_score = float(probas[1]) if len(probas) > 1 else float(probas[0])
                else:
                    fraud_score = float(self._model.predict(X)[0])
                is_fraud_predicted = fraud_score >= self._threshold

        except Exception as e:
            logger.warning(f"ML scoring failed for txn {features.get('TransactionID')}: {e}")
            fraud_score = 0.0
            is_fraud_predicted = False

        latency_ms = int((time.time() - start_time) * 1000)

        if fraud_score >= 0.8:
            risk_indication = "Critical"
            action_taken = "Block"
        elif fraud_score >= 0.6:
            risk_indication = "High"
            action_taken = "Flag"
        elif fraud_score >= 0.4:
            risk_indication = "Medium"
            action_taken = "Step-Up-Auth"
        else:
            risk_indication = "Low"
            action_taken = "Pass"

        fraud_type = self._infer_fraud_type(features) if is_fraud_predicted else None

        return {
            "fraud_score": round(fraud_score, 4),
            "is_fraud_predicted": is_fraud_predicted,
            "confidence_score": round(abs(fraud_score - 0.5) * 2, 4),
            "fraud_type": fraud_type,
            "risk_indication": risk_indication,
            "action_taken": action_taken,
            "inference_latency_ms": latency_ms,
        }

    @staticmethod
    def _infer_fraud_type(features: dict) -> str:
        """Infer the most likely fraud type based on feature values."""
        if features.get("location_mismatch", 0) >= 2:
            return "Account-Takeover"
        if features.get("is_online", 0) and features.get("is_high_value", 0):
            return "Card-Not-Present"
        if features.get("is_atm", 0) and features.get("is_night_transaction", 0):
            return "ATM-Skimming"
        if features.get("is_new_account", 0) and features.get("is_high_value", 0):
            return "New-Account-Fraud"
        if features.get("is_dormant_account", 0):
            return "Dormant-Account-Takeover"
        return "Suspicious-Transaction"


from src.infrastructure.sinks import S3SinkMapFunction


# ---------------------------------------------------------------------------
# Flink MapFunction — enrichment + feature engineering + ML scoring
# ---------------------------------------------------------------------------

class EnrichAndEngineerFunction(MapFunction):
    """
    Stateful map function that:
    1. Parses the raw Kafka JSON into a transaction dict
    2. Filters to only CDC creates ('c') and updates ('u')
    3. Enriches with Account + Customer via Redis (MSSQL fallback)
    4. Applies feature engineering
    5. Runs ML model scoring (Isolation Forest or supervised)
    6. Returns the final JSON string with enriched features + ML output
    """

    def __init__(self, redis_cfg: dict, mssql_cfg: dict, ml_cfg: dict):
        self._redis_cfg = redis_cfg
        self._mssql_cfg = mssql_cfg
        self._ml_cfg = ml_cfg
        self._lookup: Optional[DimensionLookup] = None
        self._aggregator: Optional[RedisAggregator] = None
        self._scorer: Optional[FraudModelScorer] = None

    def open(self, runtime_context):
        """Called once when the task starts — open connections and load model."""
        self._lookup = DimensionLookup(self._redis_cfg, self._mssql_cfg)
        self._aggregator = RedisAggregator(self._redis_cfg)
        self._scorer = FraudModelScorer(
            model_path=self._ml_cfg.get('model_path', '/opt/ml/models/fraud_model.joblib'),
            threshold=float(self._ml_cfg.get('threshold', '0.5')),
            model_name=self._ml_cfg.get('model_name', 'FraudGuard'),
            model_version=self._ml_cfg.get('model_version', 'v1.0.0'),
            bootstrap_samples=int(self._ml_cfg.get('bootstrap_samples', '10000')),
        )
        self._scorer.load()
        logger.info("EnrichAndEngineerFunction opened (Redis + Aggregator + MSSQL + ML model ready)")

        # --- Dimension data healthcheck ---
        # Verify that Redis has dimension data loaded so lookup failures
        # are immediately obvious in logs instead of silently producing
        # empty enrichment fields.
        try:
            r = self._lookup._get_redis()
            acct_prefix = self._redis_cfg.get('account_prefix', 'account:')
            cust_prefix = self._redis_cfg.get('customer_prefix', 'customer:')
            sample_acct = r.keys(f"{acct_prefix}*")  # KEYS is fine for a startup check
            sample_cust = r.keys(f"{cust_prefix}*")
            if not sample_acct:
                logger.warning(
                    "⚠️  DIMENSION HEALTHCHECK: No account keys found in Redis "
                    f"(prefix='{acct_prefix}'). Customer/Account enrichment will "
                    "be EMPTY. Run redis_cache_loader.py to populate the cache."
                )
            else:
                logger.info(f"✓ Dimension healthcheck: {len(sample_acct)} account key(s) in Redis")
            if not sample_cust:
                logger.warning(
                    "⚠️  DIMENSION HEALTHCHECK: No customer keys found in Redis "
                    f"(prefix='{cust_prefix}'). Customer enrichment will be EMPTY. "
                    "Run redis_cache_loader.py to populate the cache."
                )
            else:
                logger.info(f"✓ Dimension healthcheck: {len(sample_cust)} customer key(s) in Redis")
        except Exception as e:
            logger.warning(f"Dimension healthcheck failed (non-fatal): {e}")

    def close(self):
        """Called once when the task ends — close connections."""
        if self._lookup:
            self._lookup.close()
        if self._aggregator:
            self._aggregator.close()
        logger.info("EnrichAndEngineerFunction closed")

    def map(self, value: str) -> str:
        try:
            txn = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"Skipping unparseable message: {value[:200]}")
            return None

        # Filter: only process creates and updates
        # If __op is present (Debezium CDC), only allow 'c' (create) and 'u' (update)
        # If __op is absent (already unwrapped by ExtractNewRecordState), pass through
        op = txn.get("__op")
        if op is not None and op not in ("c", "u"):
            return None

        # ---- Enrich with Account data ----
        account_number = txn.get("AccountNumber")
        account = self._lookup.lookup_account(account_number)
        if account:
            txn["CustomerID"] = account.get("CustomerID", "")
            txn["AccountType"] = account.get("AccountType", "")
            txn["AccountStatus"] = account.get("AccountStatus", "")
            txn["CurrentBalance"] = account.get("Balance", "0")
            txn["AvailableBalance"] = account.get("AvailableBalance", "0")
            txn["AccountOpenDate"] = account.get("OpenDate", "")
            txn["LastActivityDate"] = account.get("LastActivityDate", "")
            txn["BranchID"] = account.get("BranchID", "")
            txn["AccountCurrency"] = account.get("Currency", "")
            txn["InterestRate"] = account.get("InterestRate", "0")
            txn["MinimumBalance"] = account.get("MinimumBalance", "0")
            txn["OverdraftLimit"] = account.get("OverdraftLimit", "0")
        else:
            logger.warning(f"Account lookup MISS for {account_number} — enrichment fields will be empty")

        # ---- Enrich with Customer data ----
        customer_id = txn.get("CustomerID")
        customer = self._lookup.lookup_customer(customer_id)
        if customer:
            txn["CustomerFirstName"] = customer.get("FirstName", "")
            txn["CustomerLastName"] = customer.get("LastName", "")
            txn["CustomerEmail"] = customer.get("Email", "")
            txn["CustomerDOB"] = customer.get("DateOfBirth", "")
            txn["CustomerGender"] = customer.get("Gender", "")
            txn["CustomerOccupation"] = customer.get("Occupation", "")
            txn["CustomerCity"] = customer.get("City", "")
            txn["CustomerState"] = customer.get("State", "")
            txn["CustomerCountry"] = customer.get("Country", "")
            txn["CustomerSegment"] = customer.get("CustomerSegment", "")
            txn["CustomerSince"] = customer.get("CustomerSince", "")
            txn["CustomerIsActive"] = customer.get("IsActive", "0")
            txn["CustomerCreatedDate"] = customer.get("CreatedDate", "")
        else:
            logger.warning(f"Customer lookup MISS for {customer_id} — customer enrichment fields will be empty")

        # ---- Aggregated features via Redis sorted sets ----
        # Use processing time (wall clock) for velocity sliding windows.
        #
        # WHY NOT event time (TransactionDate)?
        # CDC snapshot replay delivers transactions in TransactionID order,
        # NOT sorted by TransactionDate. With out-of-order event times
        # spanning years, the Redis sorted-set prune/count logic produces
        # chaotic results (a 2023-dated txn prune keeps 2025 entries;
        # a 2025-dated txn then prunes the 2023 entries, etc.).
        #
        # Processing time means 1h == 24h during snapshot replay (all
        # records processed within seconds). This is an inherent limitation
        # that resolves once the job catches up to real-time CDC events,
        # where transactions arrive in natural chronological order.
        account_number = txn.get("AccountNumber")
        transaction_id = txn.get("TransactionID", str(uuid.uuid4()))
        amount = safe_float(txn.get("Amount"))
        processing_epoch = time.time()

        agg = self._aggregator.record_and_query(
            account_number=account_number,
            transaction_id=transaction_id,
            amount=amount,
            txn_epoch=processing_epoch,
        )

        # ---- Feature engineering ----
        enriched = engineer_features(txn, agg=agg)

        # ---- ML model scoring ----
        ml_result = self._scorer.score(enriched)
        enriched.update(ml_result)

        return json.dumps(enriched, default=str)


class FilterNoneFunction(FlatMapFunction):
    """Filters out None values produced by the map step."""

    def flat_map(self, value):
        if value is not None:
            yield value


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

import sys
import os

# Add the 'streaming' directory to sys.path for absolute imports
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", "..")) # Assuming 'jobs' is inside 'streaming' and 'streaming' is at project root
sys.path.append(project_root)

from utils.config_loader import load_flat_config

def load_config():
    """Load configuration using centralized module."""
    import os
    # In Docker, the .ini file is mounted directly to /opt/flink-apps/config.ini
    docker_cfg = "/opt/flink-apps/config.ini"
    if os.path.exists(docker_cfg):
        return load_flat_config(docker_cfg)
        
    # Local fallback
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(os.path.dirname(os.path.dirname(script_dir)), "config", "flink.ini")
    return load_flat_config(config_path)


def main():
    """Main entry point for the Flink job."""
    logger.info("=" * 60)
    logger.info("Transaction CDC Enrichment + ML Scoring — Flink + Redis")
    logger.info("=" * 60)

    config = load_config()

    # ---- Set up Flink execution environment ----
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(config['parallelism'])

    # Enable checkpointing
    env.enable_checkpointing(config['checkpoint_interval_ms'])

    # Add required Flink connector JARs (expected in /opt/flink/lib)
    jar_dir = "/opt/flink/lib"
    jar_files = [
        "flink-sql-connector-kafka-3.1.0-1.18.jar",
    ]
    for jar in jar_files:
        jar_path = os.path.join(jar_dir, jar)
        if os.path.exists(jar_path):
            env.add_jars(f"file://{jar_path}")
            logger.info(f"Added JAR: {jar_path}")
        else:
            logger.warning(f"JAR not found (will try classpath): {jar_path}")

    # Add Python dependencies to Beam workers
    # We must explicitly add the streaming directories so TaskManagers can import them
    script_dir = os.path.dirname(os.path.abspath(__file__))
    streaming_dir = os.path.dirname(script_dir)
    env.add_python_file(streaming_dir)
    logger.info("Added Python files to Flink execution environment")

    # ---- Kafka source (input only) ----
    offsets = (
        KafkaOffsetsInitializer.earliest()
        if config['starting_offsets'] == 'earliest'
        else KafkaOffsetsInitializer.latest()
    )

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(config['kafka_brokers'])
        .set_topics(config['kafka_topic'])
        .set_group_id(config['consumer_group'])
        .set_starting_offsets(offsets)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    logger.info(f"Kafka source: {config['kafka_topic']} @ {config['kafka_brokers']}")

    # ---- Build pipeline ----
    watermark = WatermarkStrategy.for_monotonous_timestamps()
    source_stream = env.from_source(kafka_source, watermark, "KafkaCDCSource")

    # Dimension lookup configs (passed to map function)
    redis_cfg = {
        'host': config['redis_host'],
        'port': str(config['redis_port']),
        'db': str(config['redis_db']),
        'password': config['redis_password'],
        'ttl': str(config['redis_ttl']),
        'customer_prefix': config['redis_customer_prefix'],
        'account_prefix': config['redis_account_prefix'],
    }
    mssql_cfg = {
        'host': config['mssql_host'],
        'port': config['mssql_port'],
        'database': config['mssql_database'],
        'user': config['mssql_user'],
        'password': config['mssql_password'],
        'driver': config['mssql_driver'],
    }
    ml_cfg = {
        'model_path': config['ml_model_path'],
        'threshold': str(config['ml_threshold']),
        'model_name': config['ml_model_name'],
        'model_version': config['ml_model_version'],
        'bootstrap_samples': str(config['ml_bootstrap_samples']),
    }

    # Enrich + feature engineer + ML score
    enriched_stream = (
        source_stream
        .map(EnrichAndEngineerFunction(redis_cfg, mssql_cfg, ml_cfg),
             output_type=Types.STRING())
        .flat_map(FilterNoneFunction(), output_type=Types.STRING())
        .name("Enrich-Engineer-Score")
    )

    # ---- Sinks ----
    output_mode = config['output_mode']

    if output_mode == 's3':
        s3_cfg = {
            'bucket': config['s3_bucket'],
            'region': config['s3_region'],
            'access_key_id': config['s3_access_key_id'],
            'secret_access_key': config['s3_secret_access_key'],
            'transaction_prefix': config['s3_transaction_prefix'],
            'fraud_detection_prefix': config['s3_fraud_detection_prefix'],
        }
        s3_sink = S3SinkMapFunction(
            s3_cfg=s3_cfg,
            ml_cfg=ml_cfg,
            batch_size=config['s3_batch_size'],
            flush_interval_seconds=config['s3_flush_interval'],
        )
        # MapFunction writes to S3 as side-effect, print() ensures
        # Flink has a terminal sink (required for the pipeline to execute)
        enriched_stream \
            .map(s3_sink, output_type=Types.STRING()) \
            .name("S3ParquetSink") \
            .print() \
            .name("SinkLog")
        logger.info(
            f"S3 Parquet sink: s3://{config['s3_bucket']}/"
            f"{config['s3_transaction_prefix']}"
        )

    elif output_mode == 'console':
        enriched_stream.print().name("ConsoleSink")
        logger.info("Console sink enabled (debug mode)")

    else:
        logger.warning(
            f"Unknown output mode '{output_mode}'. "
            f"Valid modes: s3, console"
        )
        enriched_stream.print().name("FallbackConsoleSink")

    # ---- Execute ----
    logger.info("Starting Flink pipeline...")
    env.execute("TransactionCDCEnrichment-MLScoring")


if __name__ == "__main__":
    main()
