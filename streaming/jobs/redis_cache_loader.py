"""
Redis Cache Loader for Dimension Tables
========================================

Pre-populates Redis with Customer and Account dimension data from MSSQL.
Run this before starting the Flink job to warm the cache, or schedule
it periodically (e.g., every hour via cron/Airflow) to keep data fresh.

Usage:
    python redis_cache_loader.py [--config /path/to/config.ini]
"""

import redis

try:
    import pyodbc
except ImportError:
    pyodbc = None
import json
import logging
import argparse
import configparser
import os
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_mssql_connection(config: dict):
    """Create MSSQL connection using pyodbc."""
    conn_str = (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['host']},{config['port']};"
        f"DATABASE={config['database']};"
        f"UID={config['user']};"
        f"PWD={config['password']};"
        f"TrustServerCertificate=yes;"
    )
    logger.info(f"Connecting to MSSQL at {config['host']}:{config['port']}/{config['database']}")
    return pyodbc.connect(conn_str)


def serialize_value(value):
    """Serialize a value for Redis storage."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


def load_customers(mssql_conn, redis_client, prefix: str, ttl: int):
    """Load Customer dimension data into Redis."""
    logger.info("Loading Customer data from MSSQL into Redis...")

    cursor = mssql_conn.cursor()
    cursor.execute("""
        SELECT 
            CustomerID, FirstName, LastName, Email,
            DateOfBirth, Gender, MaritalStatus, Occupation,
            StreetAddress, City, State, Country, PostalCode,
            CustomerSince, CustomerSegment,
            IDType, IDNumber, TaxID,
            IsActive, CreatedDate, ModifiedDate
        FROM dbo.Customer
    """)

    columns = [desc[0] for desc in cursor.description]
    count = 0
    pipe = redis_client.pipeline()

    for row in cursor.fetchall():
        record = dict(zip(columns, row))
        customer_id = record['CustomerID']
        key = f"{prefix}{customer_id}"

        # Store as a Redis hash with all fields serialized to strings
        hash_data = {col: serialize_value(val) for col, val in record.items()}
        pipe.hset(key, mapping=hash_data)
        if ttl > 0:
            pipe.expire(key, ttl)

        count += 1
        if count % 1000 == 0:
            pipe.execute()
            pipe = redis_client.pipeline()
            logger.info(f"  Loaded {count} customers...")

    pipe.execute()
    logger.info(f"✓ Loaded {count} customers into Redis with prefix '{prefix}' (TTL={ttl}s)")
    return count


def load_accounts(mssql_conn, redis_client, prefix: str, ttl: int):
    """Load Account dimension data into Redis."""
    logger.info("Loading Account data from MSSQL into Redis...")

    cursor = mssql_conn.cursor()
    cursor.execute("""
        SELECT 
            AccountNumber, CustomerID, AccountType, BranchID,
            Balance, AvailableBalance, Currency,
            OpenDate, LastActivityDate,
            InterestRate, MinimumBalance, OverdraftLimit,
            AccountStatus,
            CreatedDate, ModifiedDate
        FROM dbo.Account
    """)

    columns = [desc[0] for desc in cursor.description]
    count = 0
    pipe = redis_client.pipeline()

    for row in cursor.fetchall():
        record = dict(zip(columns, row))
        account_number = record['AccountNumber']
        key = f"{prefix}{account_number}"

        hash_data = {col: serialize_value(val) for col, val in record.items()}
        pipe.hset(key, mapping=hash_data)
        if ttl > 0:
            pipe.expire(key, ttl)

        count += 1
        if count % 1000 == 0:
            pipe.execute()
            pipe = redis_client.pipeline()
            logger.info(f"  Loaded {count} accounts...")

    pipe.execute()
    logger.info(f"✓ Loaded {count} accounts into Redis with prefix '{prefix}' (TTL={ttl}s)")
    return count


def main():
    if pyodbc is None:
        logger.error("pyodbc is not installed. Cannot load cache from MSSQL. "
                      "Install pyodbc and ODBC Driver 17 to use this script.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description='Load MSSQL dimension tables into Redis')
    parser.add_argument('--config', default=None, help='Path to config.ini')
    parser.add_argument('--redis-host', default=None,
                        help='Override Redis host (e.g. localhost for running outside Docker)')
    parser.add_argument('--mssql-host', default=None,
                        help='Override MSSQL host (e.g. localhost for running outside Docker)')
    args = parser.parse_args()

    # Load configuration
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # redis_cache_loader is now in streaming/jobs/
    # default config is in ../../config/flink.ini
    config_path = args.config or os.path.join(os.path.dirname(script_dir), "config", "flink.ini")

    # Add streaming directory to path to allow absolute imports
    streaming_dir = os.path.dirname(script_dir)
    if streaming_dir not in sys.path:
        sys.path.append(streaming_dir)

    from utils.config_loader import load_nested_config
    cfg = load_nested_config(config_path)

    mssql_config = {
        'host': args.mssql_host or cfg.get('mssql', {}).get('host', 'host.docker.internal'),
        'port': cfg.get('mssql', {}).get('port', '1433'),
        'database': cfg.get('mssql', {}).get('database', 'BankingSystem'),
        'user': cfg.get('mssql', {}).get('user', 'sa'),
        'password': cfg.get('mssql', {}).get('password', ''),
        'driver': cfg.get('mssql', {}).get('driver', 'ODBC Driver 17 for SQL Server'),
    }

    redis_config = {
        'host': args.redis_host or cfg.get('redis', {}).get('host', 'redis'),
        'port': int(cfg.get('redis', {}).get('port', 6379)),
        'db': int(cfg.get('redis', {}).get('db', 0)),
        'password': cfg.get('redis', {}).get('password', '') or None,
    }
    ttl = int(cfg.get('redis', {}).get('ttl', 3600))
    customer_prefix = cfg.get('redis', {}).get('customer_prefix', 'customer:')
    account_prefix = cfg.get('redis', {}).get('account_prefix', 'account:')

    # Connect to Redis
    logger.info(f"Connecting to Redis at {redis_config['host']}:{redis_config['port']}")
    redis_client = redis.Redis(
        host=redis_config['host'],
        port=redis_config['port'],
        db=redis_config['db'],
        password=redis_config['password'],
        decode_responses=True
    )
    redis_client.ping()
    logger.info("✓ Redis connection successful")

    # Connect to MSSQL
    mssql_conn = get_mssql_connection(mssql_config)
    logger.info("✓ MSSQL connection successful")

    try:
        customer_count = load_customers(mssql_conn, redis_client, customer_prefix, ttl)
        account_count = load_accounts(mssql_conn, redis_client, account_prefix, ttl)

        logger.info("=" * 50)
        logger.info(f"Cache loading complete!")
        logger.info(f"  Customers: {customer_count}")
        logger.info(f"  Accounts:  {account_count}")
        logger.info(f"  TTL:       {ttl}s")
        logger.info("=" * 50)
    except Exception as e:
        logger.error(f"Error loading cache: {e}", exc_info=True)
        sys.exit(1)
    finally:
        mssql_conn.close()
        redis_client.close()


if __name__ == "__main__":
    main()
