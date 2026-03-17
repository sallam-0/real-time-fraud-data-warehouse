import os
import configparser
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Docker secrets helper — reads the .env secret from /run/secrets/env_file
# ---------------------------------------------------------------------------

_DOCKER_SECRET_PATH = "/run/secrets/env_file"
_secrets_cache: dict = None


def _load_docker_secrets() -> dict:
    """Parse the Docker secret file (mounted .env) into a dict of key=value pairs.

    The file follows standard .env format:
       KEY=VALUE
       # comments and blank lines are ignored

    Results are cached after first read.
    """
    global _secrets_cache
    if _secrets_cache is not None:
        return _secrets_cache

    _secrets_cache = {}
    if not os.path.isfile(_DOCKER_SECRET_PATH):
        return _secrets_cache

    try:
        with open(_DOCKER_SECRET_PATH, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    _secrets_cache[key.strip()] = value.strip()
        logger.info(f"Loaded {len(_secrets_cache)} secret(s) from Docker secret")
    except Exception as e:
        logger.warning(f"Failed to read Docker secret at {_DOCKER_SECRET_PATH}: {e}")

    return _secrets_cache


def _get_secret(key: str, fallback: str = "") -> str:
    """Resolve a value with priority:
       1. Docker secret file (/run/secrets/env_file)
       2. Environment variable (same key name)
       3. Fallback value
    """
    secrets = _load_docker_secrets()
    # 1. Docker secret
    val = secrets.get(key, "")
    if val:
        return val
    # 2. Environment variable
    val = os.getenv(key, "")
    if val:
        return val
    # 3. Fallback
    return fallback


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_flat_config(config_path=None) -> dict:
    """Load configuration from config.ini as a flat dictionary, mapping sections."""
    if not config_path:
        script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(script_dir, "config.ini")

    parser = configparser.ConfigParser()
    if os.path.exists(config_path):
        logger.info(f"Reading configuration from {config_path}")
        parser.read(config_path)
    else:
        logger.warning(f"Config file not found at {config_path}, using defaults")

    config = {
        # Kafka (input only — no output to Kafka)
        'kafka_brokers': _get_secret('KAFKA_BROKERS') or parser.get('kafka', 'bootstrap_servers', fallback='kafka:9092'),
        'kafka_topic': parser.get('kafka', 'input_topic', fallback='bank_cdc.BankingSystem.dbo.Transaction'),
        'consumer_group': parser.get('kafka', 'consumer_group', fallback='flink-cdc-enrichment'),
        'starting_offsets': parser.get('kafka', 'starting_offsets', fallback='latest'),
        # MSSQL
        'mssql_host': _get_secret('MSSQL_HOST') or parser.get('mssql', 'host', fallback='host.docker.internal'),
        'mssql_port': _get_secret('MSSQL_PORT') or parser.get('mssql', 'port', fallback='1433'),
        'mssql_database': _get_secret('MSSQL_DB') or parser.get('mssql', 'database', fallback='BankingSystem'),
        'mssql_user': _get_secret('MSSQL_USER') or parser.get('mssql', 'user', fallback='sa'),
        'mssql_password': _get_secret('MSSQL_PASSWORD'),
        'mssql_driver': parser.get('mssql', 'driver', fallback='ODBC Driver 17 for SQL Server'),
        # Redis
        'redis_host': _get_secret('REDIS_HOST') or parser.get('redis', 'host', fallback='redis'),
        'redis_port': parser.getint('redis', 'port', fallback=6379),
        'redis_db': parser.getint('redis', 'db', fallback=0),
        'redis_password': _get_secret('REDIS_PASSWORD') or parser.get('redis', 'password', fallback=''),
        'redis_ttl': parser.getint('redis', 'ttl', fallback=3600),
        'redis_customer_prefix': parser.get('redis', 'customer_prefix', fallback='customer:'),
        'redis_account_prefix': parser.get('redis', 'account_prefix', fallback='account:'),
        # Output (s3 = primary, console = debug only)
        'output_mode': parser.get('output', 'mode', fallback='s3'),
        # S3
        's3_bucket': _get_secret('S3_BUCKET') or parser.get('s3', 'bucket', fallback='fraud-dwh-data-lake'),
        's3_region': _get_secret('AWS_DEFAULT_REGION') or parser.get('s3', 'region', fallback='us-east-1'),
        's3_access_key_id': _get_secret('AWS_ACCESS_KEY_ID'),
        's3_secret_access_key': _get_secret('AWS_SECRET_ACCESS_KEY'),
        's3_transaction_prefix': parser.get('s3', 'transaction_prefix', fallback='real-time/transactions'),
        's3_fraud_detection_prefix': parser.get('s3', 'fraud_detection_prefix', fallback='real-time/fraud-detection'),
        's3_batch_size': parser.getint('s3', 'batch_size', fallback=100),
        's3_flush_interval': parser.getint('s3', 'flush_interval_seconds', fallback=60),
        # ML
        'ml_model_path': parser.get('ml', 'model_path', fallback='/opt/ml/models/fraud_model.joblib'),
        'ml_threshold': parser.getfloat('ml', 'threshold', fallback=0.5),
        'ml_model_name': parser.get('ml', 'model_name', fallback='FraudGuard-IsoForest'),
        'ml_model_version': parser.get('ml', 'model_version', fallback='v1.0.0'),
        'ml_bootstrap_samples': parser.getint('ml', 'bootstrap_samples', fallback=10000),
        # Flink
        'parallelism': parser.getint('flink', 'parallelism', fallback=2),
        'checkpoint_interval_ms': parser.getint('flink', 'checkpoint_interval_ms', fallback=60000),
        'state_backend': parser.get('flink', 'state_backend', fallback='hashmap'),
    }
    return config


def load_nested_config(config_path: str = None) -> dict:
    """Read config.ini and return a nested dict of sections (used by train_fraud_model and caching)."""
    if not config_path:
        script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(script_dir, "config.ini")

    parser = configparser.ConfigParser()
    parser.read(config_path)
    cfg = {}
    for section in parser.sections():
        cfg[section] = dict(parser.items(section))

    # Inject secrets from Docker secret file (highest priority)
    if 'mssql' in cfg:
        cfg['mssql']['host'] = _get_secret('MSSQL_HOST') or cfg['mssql'].get('host', 'host.docker.internal')
        cfg['mssql']['user'] = _get_secret('MSSQL_USER') or cfg['mssql'].get('user', 'sa')
        cfg['mssql']['password'] = _get_secret('MSSQL_PASSWORD')

    if 'redis' in cfg:
        cfg['redis']['host'] = _get_secret('REDIS_HOST') or cfg['redis'].get('host', 'redis')
        cfg['redis']['password'] = _get_secret('REDIS_PASSWORD') or cfg['redis'].get('password', '')

    if 'snowflake' in cfg:
        cfg['snowflake']['user'] = _get_secret('SNOWFLAKE_USER') or cfg['snowflake'].get('user', 'SALLAM')
        cfg['snowflake']['password'] = _get_secret('SNOWFLAKE_PASSWORD')

    if 's3' in cfg:
        cfg['s3']['access_key_id'] = _get_secret('AWS_ACCESS_KEY_ID')
        cfg['s3']['secret_access_key'] = _get_secret('AWS_SECRET_ACCESS_KEY')

    return cfg
