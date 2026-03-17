import logging
import hashlib
import redis
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Redis Aggregator — sliding window counters for velocity & amount features
# ---------------------------------------------------------------------------

class RedisAggregator:
    """
    Uses Redis sorted sets to track per-account transaction velocity and
    amounts over sliding time windows (1h and 24h).

    For each account, we maintain two sorted sets:
      - txn_ts:{AccountNumber}   — members are TransactionIDs, scores are epoch timestamps
      - txn_amt:{AccountNumber}  — members are TransactionIDs, scores are amounts

    On each new transaction we:
      1. Add the new entry to both sorted sets
      2. Prune entries older than 24 hours
      3. Query counts and sums for the 1h and 24h windows
    """

    WINDOW_1H = 3600
    WINDOW_24H = 86400

    def __init__(self, redis_cfg: dict):
        self._redis_cfg = redis_cfg
        self._redis: Optional[redis.Redis] = None

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

    def record_and_query(
        self,
        account_number: str,
        transaction_id: str,
        amount: float,
        txn_epoch: float,
    ) -> dict:
        """
        Record a new transaction and return aggregated stats.

        Returns dict with:
            transactions_last_1h, transactions_last_24h,
            amount_last_1h, amount_last_24h, avg_amount_24h
        """
        if not account_number:
            return self._empty_agg(amount)

        r = self._get_redis()
        ts_key = f"txn_ts:{account_number}"
        amt_key = f"txn_amt:{account_number}"

        cutoff_24h = txn_epoch - self.WINDOW_24H
        cutoff_1h = txn_epoch - self.WINDOW_1H

        try:
            pipe = r.pipeline()
            # 1. Add current transaction
            #    ts_key: member=transaction_id, score=epoch (for counting)
            #    amt_key: member="txn_id:amount", score=epoch (for amount sums)
            #    Both use epoch as score so pruning by time works correctly.
            amt_member = f"{transaction_id}:{amount}"
            pipe.zadd(ts_key, {transaction_id: txn_epoch})
            pipe.zadd(amt_key, {amt_member: txn_epoch})
            # 2. Prune entries older than 24h
            pipe.zremrangebyscore(ts_key, '-inf', cutoff_24h)
            pipe.zremrangebyscore(amt_key, '-inf', cutoff_24h)
            # 3. Set TTL so keys auto-expire if account goes dormant
            pipe.expire(ts_key, self.WINDOW_24H + 3600)
            pipe.expire(amt_key, self.WINDOW_24H + 3600)
            # 4. Count transactions in 1h and 24h windows
            pipe.zcount(ts_key, cutoff_1h, '+inf')
            pipe.zcount(ts_key, cutoff_24h, '+inf')
            # 5. Get all amount members in 24h window (score = epoch)
            pipe.zrangebyscore(amt_key, cutoff_24h, '+inf', withscores=True)
            # 6. Get all amount members in 1h window
            pipe.zrangebyscore(amt_key, cutoff_1h, '+inf', withscores=True)
            results = pipe.execute()

            # results indices: 0=zadd, 1=zadd, 2=zrem, 3=zrem,
            #   4=expire, 5=expire, 6=zcount_1h, 7=zcount_24h,
            #   8=amounts_24h, 9=amounts_1h
            txn_count_1h = results[6] or 0
            txn_count_24h = results[7] or 0

            # Parse amounts from member strings (format: "txn_id:amount")
            amounts_24h = [float(m.split(':')[-1]) for m, _ in results[8]] if results[8] else []
            amounts_1h = [float(m.split(':')[-1]) for m, _ in results[9]] if results[9] else []

            total_amount_24h = sum(amounts_24h) if amounts_24h else amount
            total_amount_1h = sum(amounts_1h) if amounts_1h else amount
            avg_amount_24h = (
                total_amount_24h / len(amounts_24h)
            ) if amounts_24h else amount

            return {
                "transactions_last_1h": txn_count_1h,
                "transactions_last_24h": txn_count_24h,
                "amount_last_1h": round(total_amount_1h, 2),
                "amount_last_24h": round(total_amount_24h, 2),
                "avg_amount_24h": round(avg_amount_24h, 2),
            }

        except Exception as e:
            logger.warning(f"Redis aggregation failed for account {account_number}: {e}")
            return self._empty_agg(amount)

        return self._empty_agg(amount)

    @staticmethod
    def _empty_agg(amount: float) -> dict:
        return {
            "transactions_last_1h": 1,
            "transactions_last_24h": 1,
            "amount_last_1h": amount,
            "amount_last_24h": amount,
            "avg_amount_24h": amount,
        }

    def close(self):
        if self._redis:
            try:
                self._redis.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Feature engineering (pure Python — mirrors the old Spark column logic)
# ---------------------------------------------------------------------------

def safe_float(val, default=0.0):
    """Safely convert a value to float."""
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_int(val, default=0):
    """Safely convert a value to int."""
    if val is None or val == "":
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def safe_bool(val, default=False):
    """Safely convert a value to bool."""
    if val is None or val == "":
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("true", "1", "yes")
    return bool(val)


def parse_datetime(val):
    """Parse an ISO datetime, epoch nanos/micros/millis/seconds, or common format.

    Debezium MSSQL CDC sends datetime2 columns as epoch **nanoseconds**
    (e.g. 1749379721000000000 = 2025-06-08T...). We detect the magnitude
    and divide accordingly.
    """
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return val

    # --- Numeric epochs (int / float) ---
    if isinstance(val, (int, float)):
        try:
            if val > 1e18:       # nanoseconds  (19 digits)
                return datetime.fromtimestamp(val / 1_000_000_000, tz=timezone.utc).replace(tzinfo=None)
            elif val > 1e15:     # microseconds (16 digits)
                return datetime.fromtimestamp(val / 1_000_000, tz=timezone.utc).replace(tzinfo=None)
            elif val > 1e12:     # milliseconds (13 digits)
                return datetime.fromtimestamp(val / 1_000, tz=timezone.utc).replace(tzinfo=None)
            elif val > 1e9:      # seconds      (10 digits)
                return datetime.fromtimestamp(val, tz=timezone.utc).replace(tzinfo=None)
        except (ValueError, OSError, OverflowError):
            return None
        return None

    # --- String values ---
    if isinstance(val, str):
        # Try parsing as numeric string first
        try:
            numeric = float(val)
            if numeric > 1e18:
                return datetime.fromtimestamp(numeric / 1_000_000_000, tz=timezone.utc).replace(tzinfo=None)
            elif numeric > 1e15:
                return datetime.fromtimestamp(numeric / 1_000_000, tz=timezone.utc).replace(tzinfo=None)
            elif numeric > 1e12:
                return datetime.fromtimestamp(numeric / 1_000, tz=timezone.utc).replace(tzinfo=None)
            elif numeric > 1e9:
                return datetime.fromtimestamp(numeric, tz=timezone.utc).replace(tzinfo=None)
        except (ValueError, TypeError, OSError, OverflowError):
            pass
        # Try ISO / common datetime string formats
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                     "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f",
                     "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(val, fmt)
                return dt.replace(tzinfo=None) if dt.tzinfo else dt
            except (ValueError, TypeError):
                continue
    return None


def days_between(dt1, dt2):
    """Return number of days between two datetimes (or 0 if either is None)."""
    if dt1 is None or dt2 is None:
        return 0
    try:
        delta = dt1 - dt2
        return abs(delta.days)
    except Exception:
        return 0


def engineer_features(txn: dict, agg: Optional[dict] = None) -> dict:
    """
    Apply all feature engineering to an enriched transaction dict.

    Args:
        txn:  Enriched transaction dict (with Account + Customer data).
        agg:  Optional aggregation dict from RedisAggregator with real
              sliding-window counters. Falls back to single-txn defaults.
    Returns a new dict with ML-ready features appended.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    amount = safe_float(txn.get("Amount"))
    balance_before = safe_float(txn.get("BalanceBefore"))
    balance_after = safe_float(txn.get("BalanceAfter"))
    current_balance = safe_float(txn.get("CurrentBalance"))
    available_balance = safe_float(txn.get("AvailableBalance"))
    overdraft_limit = safe_float(txn.get("OverdraftLimit"))
    minimum_balance = safe_float(txn.get("MinimumBalance"))
    is_international = safe_bool(txn.get("IsInternational"))
    txn_type_id = safe_int(txn.get("TransactionTypeID"))
    channel = txn.get("Channel") or ""

    txn_date = parse_datetime(txn.get("TransactionDate"))
    account_open_date = parse_datetime(txn.get("AccountOpenDate"))
    last_activity_date = parse_datetime(txn.get("LastActivityDate"))
    customer_created_date = parse_datetime(txn.get("CustomerCreatedDate"))
    customer_since = parse_datetime(txn.get("CustomerSince"))

    # Time-based features
    txn_hour = txn_date.hour if txn_date else 0
    txn_dow = txn_date.isoweekday() if txn_date else 1  # 1=Mon..7=Sun
    is_weekend = 1 if txn_dow in (6, 7) else 0
    is_night = 1 if (txn_hour >= 23 or txn_hour <= 4) else 0

    # Amount-based features
    amt_to_balance = (amount / current_balance) if current_balance > 0 else 0.0
    amount_change = balance_before - balance_after
    is_high_value = 1 if amount > 10000 else 0
    amt_to_available = (amount / available_balance) if available_balance > 0 else 0.0

    # Location features
    city = txn.get("City") or ""
    country = txn.get("Country") or ""
    cust_city = txn.get("CustomerCity") or ""
    cust_country = txn.get("CustomerCountry") or ""
    is_home_city = 1 if city == cust_city else 0
    is_home_country = 1 if country == cust_country else 0
    location_mismatch = (1 - is_home_city) + (1 - is_home_country)

    # Account age features
    account_age_days = days_between(txn_date, account_open_date) if txn_date else 0
    is_new_account = 1 if account_age_days < 90 else 0
    days_since_activity = days_between(txn_date, last_activity_date) if txn_date else 0
    is_dormant = 1 if days_since_activity > 180 else 0

    # Customer age features (relative to transaction date, not current time)
    ref_date = txn_date if txn_date else now
    customer_age_days = days_between(ref_date, customer_created_date)
    customer_tenure_days = days_between(ref_date, customer_since)
    is_new_customer = 1 if customer_tenure_days < 90 else 0

    # Account/customer status
    acct_status = txn.get("AccountStatus") or ""
    is_active_account = 1 if acct_status == "Active" else 0
    cust_is_active = safe_bool(txn.get("CustomerIsActive"))
    is_active_customer = 1 if cust_is_active else 0

    # Balance utilization
    denom = current_balance + overdraft_limit
    balance_util = (amount / denom) if denom > 0 else 0.0
    below_min = 1 if current_balance < minimum_balance else 0

    # Transaction type encoding
    is_withdrawal = 1 if txn_type_id == 3 else 0
    is_transfer = 1 if txn_type_id == 4 else 0
    is_purchase = 1 if txn_type_id == 2 else 0
    is_deposit = 1 if txn_type_id == 1 else 0

    # Channel encoding
    is_atm = 1 if channel == "ATM" else 0
    is_online = 1 if channel == "Online" else 0
    is_mobile = 1 if channel == "Mobile" else 0
    is_branch = 1 if channel == "Branch" else 0

    # Risk score
    risk_base = (
        (is_high_value * 3) +
        (int(is_international) * 2) +
        (is_night * 1) +
        (location_mismatch * 2) +
        (is_new_account * 1) +
        (is_dormant * 2) +
        (below_min * 1) +
        (0 if is_active_account else 2) +
        (0 if is_active_customer else 3)
    )

    seg = txn.get("CustomerSegment") or ""
    seg_adj = -1 if seg == "Premium" else (-2 if seg == "VIP" else (0 if seg == "Standard" else 1))
    combined_risk = risk_base + seg_adj

    # Aggregated features — from Redis sliding window counters (or defaults)
    if agg is None:
        agg = {
            "transactions_last_1h": 1,
            "transactions_last_24h": 1,
            "amount_last_1h": amount,
            "amount_last_24h": amount,
            "avg_amount_24h": amount,
        }

    txn_count_1h = agg.get("transactions_last_1h", 1)
    txn_count_24h = agg.get("transactions_last_24h", 1)
    amount_1h = agg.get("amount_last_1h", amount)
    amount_24h = agg.get("amount_last_24h", amount)
    avg_amount_24h = agg.get("avg_amount_24h", amount)

    # Velocity: high if more than 5 transactions in 1h
    is_high_velocity = 1 if txn_count_1h >= 5 else 0
    # Unusual: amount is > 3x the 24h average for this account
    is_unusual_amount = 1 if (avg_amount_24h > 0 and amount > avg_amount_24h * 3) else 0

    features = {
        # Identifiers
        "TransactionID": txn.get("TransactionID"),
        "AccountNumber": txn.get("AccountNumber"),
        "CustomerID": txn.get("CustomerID"),
        "MerchantID": txn.get("MerchantID"),
        "ATMID": txn.get("ATMID"),
        "BranchID": txn.get("BranchID"),
        "CardNumberHashed": hashlib.sha256(
            (txn.get("CardNumber") or "").strip().encode()
        ).hexdigest() if txn.get("CardNumber") else None,
        "TransactionDate": txn.get("TransactionDate"),
        # Original transaction fields
        "Amount": amount,
        "Currency": txn.get("Currency"),
        "TransactionTypeID": txn_type_id,
        "TransactionMethod": txn.get("TransactionMethod"),
        "Channel": channel,
        "TransactionStatus": txn.get("TransactionStatus"),
        # Location
        "City": city,
        "State": txn.get("State"),
        "Country": country,
        "Latitude": safe_float(txn.get("Latitude")),
        "Longitude": safe_float(txn.get("Longitude")),
        "IsInternational": is_international,
        # Device
        "DeviceID": txn.get("DeviceID"),
        "DeviceType": txn.get("DeviceType"),
        "IPAddress": txn.get("IPAddress"),
        # Customer info
        "CustomerSegment": seg,
        "CustomerCity": cust_city,
        "CustomerCountry": cust_country,
        "CustomerGender": txn.get("CustomerGender"),
        "CustomerOccupation": txn.get("CustomerOccupation"),
        "CustomerIsActive": cust_is_active,
        # Account info
        "AccountType": txn.get("AccountType"),
        "AccountStatus": acct_status,
        "CurrentBalance": current_balance,
        "AvailableBalance": available_balance,
        "AccountCurrency": txn.get("AccountCurrency"),
        "InterestRate": safe_float(txn.get("InterestRate")),
        "MinimumBalance": minimum_balance,
        "OverdraftLimit": overdraft_limit,
        "account_age_days": account_age_days,
        "days_since_last_activity": days_since_activity,
        # Engineered — Time
        "transaction_hour": txn_hour,
        "transaction_day_of_week": txn_dow,
        "is_weekend": is_weekend,
        "is_night_transaction": is_night,
        # Engineered — Amount
        "amount_to_balance_ratio": round(amt_to_balance, 6),
        "amount_to_available_ratio": round(amt_to_available, 6),
        "is_high_value": is_high_value,
        "balance_utilization": round(balance_util, 6),
        "below_minimum_balance": below_min,
        # Engineered — Location
        "is_home_city": is_home_city,
        "is_home_country": is_home_country,
        "location_mismatch": location_mismatch,
        # Engineered — Account
        "is_new_account": is_new_account,
        "is_dormant_account": is_dormant,
        "customer_age_days": customer_age_days,
        "customer_tenure_days": customer_tenure_days,
        "is_new_customer": is_new_customer,
        "is_active_account": is_active_account,
        "is_active_customer": is_active_customer,
        # Engineered — Transaction type
        "is_withdrawal": is_withdrawal,
        "is_transfer": is_transfer,
        "is_purchase": is_purchase,
        "is_deposit": is_deposit,
        # Engineered — Channel
        "is_atm": is_atm,
        "is_online": is_online,
        "is_mobile": is_mobile,
        "is_branch": is_branch,
        # Engineered — Risk
        "risk_score_base": risk_base,
        "segment_risk_adjustment": seg_adj,
        "combined_risk_score": combined_risk,
        # Aggregated — from Redis sliding window counters
        "transactions_last_1h": txn_count_1h,
        "transactions_last_24h": txn_count_24h,
        "amount_last_1h": amount_1h,
        "amount_last_24h": amount_24h,
        "avg_amount_24h": avg_amount_24h,
        "is_high_velocity": is_high_velocity,
        "is_unusual_amount": is_unusual_amount,
        # Metadata
        "processing_timestamp": now.isoformat(),
    }
    return features


# Feature columns used by the ML model (must match training feature order)
ML_FEATURE_COLUMNS = [
    "Amount", "transaction_hour", "transaction_day_of_week", "is_weekend",
    "is_night_transaction", "amount_to_balance_ratio", "amount_to_available_ratio",
    "is_high_value", "balance_utilization", "below_minimum_balance",
    "is_home_city", "is_home_country", "location_mismatch",
    "is_new_account", "is_dormant_account", "customer_age_days",
    "customer_tenure_days", "is_new_customer", "is_active_account",
    "is_active_customer", "is_withdrawal", "is_transfer", "is_purchase",
    "is_deposit", "is_atm", "is_online", "is_mobile", "is_branch",
    "risk_score_base", "combined_risk_score", "account_age_days",
    "days_since_last_activity", "transactions_last_1h", "transactions_last_24h",
    "amount_last_1h", "amount_last_24h", "avg_amount_24h",
    "is_high_velocity", "is_unusual_amount",
]
