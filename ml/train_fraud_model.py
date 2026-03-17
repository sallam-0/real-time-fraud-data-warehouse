"""
Fraud Detection Model Training Script
=======================================

Trains ML models for fraud detection using two modes:

  1. UNSUPERVISED (cold start — no labels):
     - Queries MSSQL directly for raw transactions + Account + Customer
     - Sorts per account by TransactionDate for correct 1h/24h windows
     - Computes all features via engineer_features() from the Flink job
     - Trains an Isolation Forest

  2. SUPERVISED (feedback loop — labels available):
     - Same as above, but also fetches IS_FRAUD_CONFIRMED labels from
       Snowflake FACT_TRANSACTION (written back from analyst reviews)
     - Trains a Random Forest classifier with ground-truth labels
     - Reports precision, recall, F1, AUC-ROC

After training, the model is:
  - Saved as a .joblib file (loaded by the streaming Flink job)
  - Optionally registered in DIM_FRAUD_MODEL in Snowflake

Usage:
    # Cold start — unsupervised
    python train_fraud_model.py --mode unsupervised

    # Retrain with labels — supervised  (requires Snowflake [snowflake] config)
    python train_fraud_model.py --mode supervised --min-labels 500

    # Auto — picks supervised if enough labels exist, else unsupervised
    python train_fraud_model.py --mode auto
"""

import argparse
import configparser
import os
import sys
import logging
import collections
from datetime import datetime, timezone

import numpy as np
import joblib

# ---------------------------------------------------------------------------
# Reuse feature engineering from the Flink job (no duplication)
# ---------------------------------------------------------------------------
# Add the root directory to path so we can import streaming.src components
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.append(root_dir)

from streaming.src.domain.features import (
    engineer_features,
    parse_datetime,
    safe_float,
    safe_bool,
    safe_int,
    days_between,
    ML_FEATURE_COLUMNS,
)
from streaming.utils.config_loader import load_nested_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("train_fraud_model")


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _connect_mssql(config: dict):
    """Connect to MSSQL using pyodbc."""
    import pyodbc
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


def _connect_snowflake(config: dict):
    """Connect to Snowflake for label retrieval."""
    import snowflake.connector
    return snowflake.connector.connect(
        account=config.get("account", ""),
        user=config.get("user", ""),
        password=config.get("password", ""),
        database=config.get("database", "FRAUD_DWH"),
        schema=config.get("schema", "DWH"),
        warehouse=config.get("warehouse", "COMPUTE_WH"),
        role=config.get("role", ""),
    )


def _load_config(config_path: str) -> dict:
    """Read config.ini and return a nested dict of sections."""
    return load_nested_config(config_path)


# ---------------------------------------------------------------------------
# Data loading from MSSQL
# ---------------------------------------------------------------------------

def _fetch_raw_transactions(mssql_conn) -> list[dict]:
    """
    Fetch all transactions with Account + Customer JOINs from MSSQL.
    Returns a list of dicts, one per transaction.
    """
    query = """
        SELECT 
            t.TransactionID,
            t.AccountNumber,
            c.CustomerID,
            t.TransactionDate,
            t.Amount,
            t.Currency,
            t.TransactionTypeID,
            t.BalanceBefore,
            t.BalanceAfter,
            t.TransactionMethod,
            t.Channel,
            t.TransactionStatus,
            t.City,
            t.State,
            t.Country,
            t.Latitude,
            t.Longitude,
            t.IsInternational,
            t.DeviceID,
            t.DeviceType,
            t.IPAddress,
            -- Account fields
            a.AccountType,
            a.AccountStatus,
            a.Balance       AS CurrentBalance,
            a.AvailableBalance,
            a.Currency      AS AccountCurrency,
            a.OpenDate      AS AccountOpenDate,
            a.LastActivityDate,
            a.InterestRate,
            a.MinimumBalance,
            a.OverdraftLimit,
            a.BranchID,
            -- Customer fields
            c.FirstName     AS CustomerFirstName,
            c.LastName      AS CustomerLastName,
            c.Email         AS CustomerEmail,
            c.DateOfBirth   AS CustomerDOB,
            c.Gender        AS CustomerGender,
            c.MaritalStatus AS CustomerMaritalStatus,
            c.Occupation    AS CustomerOccupation,
            c.City          AS CustomerCity,
            c.Country       AS CustomerCountry,
            c.CustomerSegment,
            c.IsActive      AS CustomerIsActive,
            c.CreatedDate   AS CustomerCreatedDate,
            c.CustomerSince
        FROM dbo.[Transaction] t
        LEFT JOIN dbo.Account a ON a.AccountNumber = t.AccountNumber
        LEFT JOIN dbo.Customer c ON c.CustomerID = a.CustomerID
        ORDER BY t.AccountNumber, t.TransactionDate
    """
    cursor = mssql_conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = []
    for row in cursor.fetchall():
        record = {}
        for col, val in zip(columns, row):
            if isinstance(val, datetime):
                record[col] = val.isoformat()
            elif val is None:
                record[col] = ""
            else:
                record[col] = str(val)
        rows.append(record)
    cursor.close()
    logger.info(f"Fetched {len(rows)} transactions from MSSQL (sorted by AccountNumber, TransactionDate)")
    return rows


# ---------------------------------------------------------------------------
# In-memory sliding window aggregation (sorted by event time)
# ---------------------------------------------------------------------------

def _compute_aggregations(transactions: list[dict]) -> list[dict]:
    """
    Compute per-account sliding window aggregations in chronological order.
    
    Since transactions are sorted by (AccountNumber, TransactionDate),
    we iterate per account and maintain a deque-based sliding window
    for 1h and 24h aggregations.
    
    Returns a list of aggregation dicts (same length as input) with:
      TRANSACTION_COUNT_1H, TRANSACTION_COUNT_24H,
      AMOUNT_SUM_1H, AMOUNT_SUM_24H
    """
    import calendar as cal

    ONE_HOUR = 3600
    TWENTY_FOUR_HOURS = 86400

    aggregations = []
    # Group by account (already sorted by AccountNumber, TransactionDate)
    current_account = None
    window = collections.deque()  # (epoch, amount) pairs

    for txn in transactions:
        account = txn.get("AccountNumber", "")
        if account != current_account:
            current_account = account
            window.clear()

        # Parse transaction date to epoch
        txn_dt = parse_datetime(txn.get("TransactionDate"))
        if txn_dt is not None:
            epoch = cal.timegm(txn_dt.timetuple()) + txn_dt.microsecond / 1e6
        else:
            epoch = 0.0

        amount = safe_float(txn.get("Amount"))

        # Add current transaction to window
        window.append((epoch, amount))

        # Prune entries older than 24h (from the left since sorted)
        cutoff_24h = epoch - TWENTY_FOUR_HOURS
        while window and window[0][0] < cutoff_24h:
            window.popleft()

        # Count and sum for each window
        cutoff_1h = epoch - ONE_HOUR
        count_1h = 0
        sum_1h = 0.0
        count_24h = 0
        sum_24h = 0.0

        for ts, amt in window:
            if ts >= cutoff_24h:
                count_24h += 1
                sum_24h += amt
                if ts >= cutoff_1h:
                    count_1h += 1
                    sum_1h += amt

        aggregations.append({
            "TRANSACTION_COUNT_1H": count_1h,
            "TRANSACTION_COUNT_24H": count_24h,
            "AMOUNT_SUM_1H": round(sum_1h, 2),
            "AMOUNT_SUM_24H": round(sum_24h, 2),
        })

    return aggregations


# ---------------------------------------------------------------------------
# Feature engineering for training data
# ---------------------------------------------------------------------------

def _build_feature_matrix(transactions: list[dict], aggregations: list[dict]) -> np.ndarray:
    """
    Apply engineer_features() to each transaction with its aggregation,
    then extract the ML feature vector.
    Returns a 2D numpy array (n_samples x n_features).
    """
    feature_rows = []
    skipped = 0

    for txn, agg in zip(transactions, aggregations):
        try:
            features = engineer_features(txn, agg=agg)
            vector = []
            for col in ML_FEATURE_COLUMNS:
                val = features.get(col, 0)
                try:
                    vector.append(float(val) if val is not None else 0.0)
                except (ValueError, TypeError):
                    vector.append(0.0)
            feature_rows.append(vector)
        except Exception as e:
            skipped += 1
            if skipped <= 5:
                logger.warning(f"Skipped transaction {txn.get('TransactionID')}: {e}")

    if skipped > 0:
        logger.warning(f"Total skipped transactions: {skipped}")

    logger.info(f"Built feature matrix: {len(feature_rows)} samples x {len(ML_FEATURE_COLUMNS)} features")
    return np.array(feature_rows, dtype=float)


# ---------------------------------------------------------------------------
# Label fetching from Snowflake
# ---------------------------------------------------------------------------

def _fetch_labels_from_snowflake(sf_config: dict) -> dict:
    """
    Fetch fraud labels from Snowflake FACT_TRANSACTION.
    Returns a dict mapping TransactionID -> IS_FRAUD_CONFIRMED (0 or 1).
    """
    conn = _connect_snowflake(sf_config)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TRANSACTION_ID, IS_FRAUD_CONFIRMED
            FROM DWH.FACT_TRANSACTION
            WHERE IS_FRAUD_CONFIRMED IS NOT NULL
        """)
        labels = {}
        for row in cursor.fetchall():
            txn_id = str(row[0]).strip()
            is_fraud = 1 if row[1] else 0
            labels[txn_id] = is_fraud
        cursor.close()
        logger.info(f"Fetched {len(labels)} labeled transactions from Snowflake "
                    f"(fraud={sum(labels.values())}, legit={len(labels) - sum(labels.values())})")
        return labels
    finally:
        conn.close()


def _count_labels_in_snowflake(sf_config: dict) -> int:
    """Count labeled rows in Snowflake (for auto mode)."""
    conn = _connect_snowflake(sf_config)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM DWH.FACT_TRANSACTION "
            "WHERE IS_FRAUD_CONFIRMED IS NOT NULL"
        )
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Model training
# ---------------------------------------------------------------------------

def _train_isolation_forest(X: np.ndarray, contamination: float = 0.05):
    """Train an Isolation Forest (unsupervised cold start)."""
    from sklearn.ensemble import IsolationForest

    logger.info(f"Training Isolation Forest on {X.shape[0]} samples, "
                f"{X.shape[1]} features, contamination={contamination}")

    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        max_samples="auto",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X)

    preds = model.predict(X)
    n_anomalies = int((preds == -1).sum())
    logger.info(f"Training complete. Anomalies: {n_anomalies}/{len(preds)} "
                f"({100 * n_anomalies / len(preds):.1f}%)")

    return model, {
        "algorithm": "IsolationForest",
        "n_estimators": 200,
        "contamination": contamination,
        "training_samples": X.shape[0],
        "anomalies_detected": n_anomalies,
    }


def _train_random_forest(X: np.ndarray, y: np.ndarray):
    """Train a Random Forest classifier (supervised with labels)."""
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import classification_report, roc_auc_score

    fraud_ratio = y.mean()
    logger.info(f"Training Random Forest on {X.shape[0]} samples, "
                f"fraud ratio: {fraud_ratio:.3f}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=15,
        min_samples_split=10,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    # Evaluate
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_proba)
    report = classification_report(y_test, y_pred)

    logger.info(f"\n{report}")
    logger.info(f"AUC-ROC: {auc:.4f}")

    # Feature importance top 10
    importances = model.feature_importances_
    top_10_idx = np.argsort(importances)[::-1][:10]
    logger.info("Top 10 feature importances:")
    for i, idx in enumerate(top_10_idx, 1):
        logger.info(f"  {i}. {ML_FEATURE_COLUMNS[idx]}: {importances[idx]:.4f}")

    return model, {
        "algorithm": "RandomForestClassifier",
        "n_estimators": 300,
        "max_depth": 15,
        "training_samples": int(X_train.shape[0]),
        "test_samples": int(X_test.shape[0]),
        "auc_roc": round(auc, 4),
        "fraud_ratio": round(fraud_ratio, 4),
    }


# ---------------------------------------------------------------------------
# Snowflake model registration
# ---------------------------------------------------------------------------

def _register_model_in_snowflake(sf_config: dict, ml_config: dict, metrics: dict):
    """Insert the trained model into DIM_FRAUD_MODEL in Snowflake."""
    try:
        conn = _connect_snowflake(sf_config)
        cursor = conn.cursor()

        model_name = ml_config.get("model_name", "FraudGuard")
        model_version = ml_config.get("model_version", "v1.0.0")
        threshold = float(ml_config.get("threshold", 0.5))

        # Deactivate previous models
        cursor.execute(
            "UPDATE DWH.DIM_FRAUD_MODEL SET IS_ACTIVE = FALSE "
            "WHERE IS_ACTIVE = TRUE"
        )

        # Insert new model
        cursor.execute("""
            INSERT INTO DWH.DIM_FRAUD_MODEL (
                MODEL_NAME, MODEL_VERSION, ALGORITHM, FRAMEWORK,
                TRAINING_DATE, THRESHOLD_SCORE, IS_ACTIVE,
                AUC_ROC, FEATURE_LIST, DESCRIPTION
            ) VALUES (
                %s, %s, %s, 'scikit-learn',
                CURRENT_TIMESTAMP(), %s, TRUE,
                %s, %s, %s
            )
        """, (
            model_name, model_version,
            metrics.get("algorithm", "IsolationForest"),
            threshold,
            metrics.get("auc_roc"),
            ", ".join(ML_FEATURE_COLUMNS),
            f"Trained on {metrics.get('training_samples', 0)} samples. "
            f"Algorithm: {metrics.get('algorithm')}",
        ))

        conn.commit()
        logger.info(f"Model registered in DIM_FRAUD_MODEL: {model_name} {model_version}")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not register model in Snowflake: {e}")
        logger.warning("Model is saved locally — registration can be done later.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Train fraud detection model (unsupervised/supervised)"
    )
    parser.add_argument(
        "--mode", choices=["unsupervised", "supervised", "auto"],
        default="auto",
        help="Training mode (default: auto)"
    )
    parser.add_argument(
        "--config", default=None,
        help="Path to config.ini (default: flink-jobs/config.ini)"
    )
    parser.add_argument(
        "--output", default=None,
        help="Output path for model file"
    )
    parser.add_argument(
        "--min-labels", type=int, default=500,
        help="Minimum labeled samples for supervised mode (default: 500)"
    )
    parser.add_argument(
        "--contamination", type=float, default=0.05,
        help="Contamination ratio for Isolation Forest (default: 0.05)"
    )
    parser.add_argument(
        "--mssql-host", default=None,
        help="Override MSSQL host (for running outside Docker)"
    )
    parser.add_argument(
        "--register", action="store_true",
        help="Register the model in Snowflake DIM_FRAUD_MODEL"
    )
    args = parser.parse_args()

    # Resolve paths
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Default to ../config/flink.ini relative to this file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = args.config or os.path.join(os.path.dirname(script_dir), "config", "flink.ini")
    output_path = args.output or os.path.join(project_root, "models", "fraud_model.joblib")

    logger.info("=" * 60)
    logger.info("Fraud Detection Model Training")
    logger.info("=" * 60)

    # Load config
    cfg = _load_config(config_path)
    mssql_config = cfg.get("mssql", {})
    sf_config = cfg.get("snowflake", {})
    ml_config = cfg.get("ml", {})

    # Override MSSQL host if specified
    if args.mssql_host:
        mssql_config["host"] = args.mssql_host

    # Determine mode
    mode = args.mode
    if mode == "auto":
        if sf_config.get("account"):
            try:
                label_count = _count_labels_in_snowflake(sf_config)
                logger.info(f"Auto mode: found {label_count} labeled rows in Snowflake")
                mode = "supervised" if label_count >= args.min_labels else "unsupervised"
            except Exception as e:
                logger.info(f"Auto mode: Snowflake not available ({e}), using unsupervised")
                mode = "unsupervised"
        else:
            logger.info("Auto mode: no Snowflake config, using unsupervised")
            mode = "unsupervised"

    logger.info(f"Mode: {mode.upper()}")
    logger.info(f"Model: {ml_config.get('model_name', 'FraudGuard')} "
                f"{ml_config.get('model_version', 'v1.0.0')}")

    # ── Step 1: Fetch raw transactions from MSSQL ──
    mssql_conn = _connect_mssql(mssql_config)
    try:
        transactions = _fetch_raw_transactions(mssql_conn)
    finally:
        mssql_conn.close()

    if not transactions:
        logger.error("No transactions found in MSSQL. Aborting.")
        sys.exit(1)

    # ── Step 2: Compute sorted sliding window aggregations ──
    logger.info("Computing per-account sorted sliding window aggregations...")
    aggregations = _compute_aggregations(transactions)

    # Verify 1h vs 24h differentiation
    diff_count = sum(
        1 for agg in aggregations
        if agg["TRANSACTION_COUNT_1H"] != agg["TRANSACTION_COUNT_24H"]
    )
    logger.info(f"Aggregation quality: {diff_count}/{len(aggregations)} rows have "
                f"1h ≠ 24h counts ({100 * diff_count / len(aggregations):.1f}%)")

    # ── Step 3: Build feature matrix ──
    logger.info("Applying feature engineering (reusing Flink job's engineer_features)...")
    X = _build_feature_matrix(transactions, aggregations)

    # ── Step 4: Train model ──
    if mode == "supervised":
        # Fetch labels from Snowflake
        labels = _fetch_labels_from_snowflake(sf_config)
        if len(labels) < args.min_labels:
            logger.warning(
                f"Only {len(labels)} labels available (need {args.min_labels}). "
                f"Falling back to unsupervised mode."
            )
            mode = "unsupervised"
        else:
            # Match labels to feature matrix by TransactionID
            y = np.zeros(len(transactions), dtype=int)
            matched = 0
            for i, txn in enumerate(transactions):
                txn_id = txn.get("TransactionID", "")
                if txn_id in labels:
                    y[i] = labels[txn_id]
                    matched += 1

            if matched < args.min_labels:
                logger.warning(
                    f"Only {matched} labels matched to training data "
                    f"(need {args.min_labels}). Falling back to unsupervised."
                )
                mode = "unsupervised"
            else:
                # Filter to only labeled rows
                labeled_mask = np.array([
                    txn.get("TransactionID", "") in labels
                    for txn in transactions
                ])
                X_labeled = X[labeled_mask]
                y_labeled = y[labeled_mask]
                logger.info(f"Using {X_labeled.shape[0]} labeled samples for training")
                model, metrics = _train_random_forest(X_labeled, y_labeled)

    if mode == "unsupervised":
        model, metrics = _train_isolation_forest(X, contamination=args.contamination)

    # ── Step 5: Save model ──
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    joblib.dump(model, output_path)
    logger.info(f"Model saved to: {output_path}")
    logger.info(f"Model size: {os.path.getsize(output_path) / 1024:.1f} KB")

    # ── Step 6: Register in Snowflake (optional) ──
    if args.register and sf_config.get("account"):
        _register_model_in_snowflake(sf_config, ml_config, metrics)

    # ── Summary ──
    logger.info("=" * 60)
    logger.info("Training complete!")
    logger.info(f"  Mode:      {mode}")
    logger.info(f"  Algorithm: {metrics['algorithm']}")
    logger.info(f"  Samples:   {metrics['training_samples']}")
    if "auc_roc" in metrics:
        logger.info(f"  AUC-ROC:   {metrics['auc_roc']}")
    if "anomalies_detected" in metrics:
        logger.info(f"  Anomalies: {metrics['anomalies_detected']}")
    logger.info(f"  Output:    {output_path}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
