import logging
import redis
from typing import Optional
from datetime import datetime

try:
    import pyodbc
    HAS_PYODBC = True
except ImportError:
    HAS_PYODBC = False

logger = logging.getLogger(__name__)

class DimensionLookup:
    """
    Handles dimension data lookups against Redis, with MSSQL fallback on cache miss.
    Instances are created per-task (not serialized across the network).
    """

    def __init__(self, redis_cfg: dict, mssql_cfg: dict):
        self._redis_cfg = redis_cfg
        self._mssql_cfg = mssql_cfg
        self._redis: Optional[redis.Redis] = None
        self._mssql_conn = None

    # -- lazy connections (opened once per task) ----------------------------

    def _get_redis(self) -> 'redis.Redis':
        if self._redis is None:
            self._redis = redis.Redis(
                host=self._redis_cfg['host'],
                port=int(self._redis_cfg['port']),
                db=int(self._redis_cfg.get('db', 0)),
                password=self._redis_cfg.get('password') or None,
                decode_responses=True,
                socket_connect_timeout=5,
            )
        return self._redis

    def _get_mssql(self):
        if not HAS_PYODBC:
            return None
        if self._mssql_conn is None:
            conn_str = (
                f"DRIVER={{{self._mssql_cfg['driver']}}};"
                f"SERVER={self._mssql_cfg['host']},{self._mssql_cfg['port']};"
                f"DATABASE={self._mssql_cfg['database']};"
                f"UID={self._mssql_cfg['user']};"
                f"PWD={self._mssql_cfg['password']};"
                f"TrustServerCertificate=yes;"
            )
            self._mssql_conn = pyodbc.connect(conn_str)
        return self._mssql_conn

    # -- public lookup methods ----------------------------------------------

    def lookup_account(self, account_number: str) -> Optional[dict]:
        """Look up Account data from Redis; fall back to MSSQL on miss."""
        if not account_number:
            return None

        r = self._get_redis()
        prefix = self._redis_cfg.get('account_prefix', 'account:')
        key = f"{prefix}{account_number}"

        data = r.hgetall(key)
        if data:
            return data

        # Cache miss → query MSSQL
        try:
            conn = self._get_mssql()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT AccountNumber, CustomerID, AccountType, BranchID, "
                "Balance, AvailableBalance, Currency, OpenDate, LastActivityDate, "
                "InterestRate, MinimumBalance, OverdraftLimit, AccountStatus, "
                "CreatedDate, ModifiedDate FROM dbo.Account WHERE AccountNumber = ?",
                account_number,
            )
            row = cursor.fetchone()
            if row is None:
                return None
            columns = [desc[0] for desc in cursor.description]
            record = {col: self._serialize(val) for col, val in zip(columns, row)}
            # Write back into Redis
            ttl = int(self._redis_cfg.get('ttl', 3600))
            r.hset(key, mapping=record)
            if ttl > 0:
                r.expire(key, ttl)
            return record
        except Exception as e:
            logger.warning(f"MSSQL fallback failed for account {account_number}: {e}")
            return None

    def lookup_customer(self, customer_id: str) -> Optional[dict]:
        """Look up Customer data from Redis; fall back to MSSQL on miss."""
        if not customer_id:
            return None

        r = self._get_redis()
        prefix = self._redis_cfg.get('customer_prefix', 'customer:')
        key = f"{prefix}{customer_id}"

        data = r.hgetall(key)
        if data:
            return data

        try:
            conn = self._get_mssql()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT CustomerID, FirstName, LastName, Email, DateOfBirth, "
                "Gender, MaritalStatus, Occupation, StreetAddress, City, State, "
                "Country, PostalCode, CustomerSince, CustomerSegment, IDType, "
                "IDNumber, TaxID, IsActive, CreatedDate, ModifiedDate "
                "FROM dbo.Customer WHERE CustomerID = ?",
                customer_id,
            )
            row = cursor.fetchone()
            if row is None:
                return None
            columns = [desc[0] for desc in cursor.description]
            record = {col: self._serialize(val) for col, val in zip(columns, row)}
            ttl = int(self._redis_cfg.get('ttl', 3600))
            r.hset(key, mapping=record)
            if ttl > 0:
                r.expire(key, ttl)
            return record
        except Exception as e:
            logger.warning(f"MSSQL fallback failed for customer {customer_id}: {e}")
            return None

    @staticmethod
    def _serialize(value):
        if value is None:
            return ""
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, bool):
            return "1" if value else "0"
        return str(value)

    def close(self):
        if self._redis:
            try:
                self._redis.close()
            except Exception:
                pass
        if self._mssql_conn:
            try:
                self._mssql_conn.close()
            except Exception:
                pass
