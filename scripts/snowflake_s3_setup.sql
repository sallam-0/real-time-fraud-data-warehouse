-- =============================================================================
--  SNOWFLAKE S3 INTEGRATION SETUP
--  Purpose : Create storage integration, stages, file formats, and
--            staging tables for loading data from AWS S3 into Snowflake.
--  Pattern : S3 (data lake) → COPY INTO → STAGING schema → dbt → DWH schema
--  Date    : 2026-02-24
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE FRAUD_DWH;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
--  1.  STORAGE INTEGRATION
--      Grants Snowflake permission to read from the S3 bucket.
--      After creation, run:
--        DESC INTEGRATION S3_FRAUD_DWH_INT;
--      Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
--      into your IAM role trust policy.
-- =============================================================================

CREATE OR REPLACE STORAGE INTEGRATION S3_FRAUD_DWH_INT
    TYPE                = EXTERNAL_STAGE
    STORAGE_PROVIDER    = 'S3'
    ENABLED             = TRUE
    STORAGE_AWS_ROLE_ARN = '<YOUR_AWS_ROLE_ARN>'         -- e.g. arn:aws:iam::123456789012:role/snowflake-s3-role
    STORAGE_ALLOWED_LOCATIONS = ('s3://aws.s3-sallam/');

-- Show the ARN + external ID to configure in your AWS IAM trust policy
DESC INTEGRATION S3_FRAUD_DWH_INT;


-- =============================================================================
--  2.  FILE FORMATS
-- =============================================================================

-- Parquet format for batch extracts and real-time Flink output
CREATE OR REPLACE FILE FORMAT STAGING.PARQUET_FORMAT
    TYPE            = PARQUET
    COMPRESSION     = SNAPPY;

-- JSON format (fallback, if any data is stored as JSON)
CREATE OR REPLACE FILE FORMAT STAGING.JSON_FORMAT
    TYPE                  = JSON
    COMPRESSION           = AUTO
    STRIP_OUTER_ARRAY     = TRUE
    IGNORE_UTF8_ERRORS    = TRUE;


-- =============================================================================
--  3.  EXTERNAL STAGES
-- =============================================================================

-- Stage for batch MSSQL extracts (Parquet)
CREATE OR REPLACE STAGE STAGING.S3_BATCH_STAGE
    STORAGE_INTEGRATION = S3_FRAUD_DWH_INT
    URL                 = 's3://aws.s3-sallameu/batch/raw/'
    FILE_FORMAT         = STAGING.PARQUET_FORMAT;

-- Stage for real-time Flink transaction output (Parquet)
CREATE OR REPLACE STAGE STAGING.S3_REALTIME_TXN_STAGE
    STORAGE_INTEGRATION = S3_FRAUD_DWH_INT
    URL                 = 's3://aws.s3-sallameu/real-time/transactions/'
    FILE_FORMAT         = STAGING.PARQUET_FORMAT;

-- Stage for real-time Flink fraud detection output (Parquet)
CREATE OR REPLACE STAGE STAGING.S3_REALTIME_DET_STAGE
    STORAGE_INTEGRATION = S3_FRAUD_DWH_INT
    URL                 = 's3://aws.s3-sallameu/real-time/fraud-detection/'
    FILE_FORMAT         = STAGING.PARQUET_FORMAT;


-- =============================================================================
--  4.  STAGING TABLES FOR REAL-TIME DATA
--      These receive Flink output via COPY INTO.
--      All columns are VARCHAR to match the Parquet string serialization
--      from the Flink S3SinkMapFunction.
-- =============================================================================

CREATE OR REPLACE TABLE STAGING.RAW_REALTIME_TRANSACTION (
    TransactionID           VARCHAR,
    AccountNumber           VARCHAR,
    CustomerID              VARCHAR,
    MerchantID              VARCHAR,
    ATMID                   VARCHAR,
    BranchID                VARCHAR,
    CardNumberHashed        VARCHAR,
    TransactionDate         VARCHAR,
    Amount                  VARCHAR,
    Currency                VARCHAR,
    TransactionTypeID       VARCHAR,
    TransactionMethod       VARCHAR,
    Channel                 VARCHAR,
    TransactionStatus       VARCHAR,
    City                    VARCHAR,
    State                   VARCHAR,
    Country                 VARCHAR,
    Latitude                VARCHAR,
    Longitude               VARCHAR,
    IsInternational         VARCHAR,
    DeviceID                VARCHAR,
    DeviceType              VARCHAR,
    IPAddress               VARCHAR,
    CustomerSegment         VARCHAR,
    CustomerCity            VARCHAR,
    CustomerCountry         VARCHAR,
    AccountType             VARCHAR,
    AccountStatus           VARCHAR,
    CurrentBalance          VARCHAR,
    AvailableBalance        VARCHAR,
    transaction_hour        VARCHAR,
    transaction_day_of_week VARCHAR,
    is_weekend              VARCHAR,
    is_night_transaction    VARCHAR,
    amount_to_balance_ratio VARCHAR,
    amount_to_available_ratio VARCHAR,
    is_high_value           VARCHAR,
    balance_utilization     VARCHAR,
    below_minimum_balance   VARCHAR,
    is_home_city            VARCHAR,
    is_home_country         VARCHAR,
    location_mismatch       VARCHAR,
    is_new_account          VARCHAR,
    is_dormant_account      VARCHAR,
    account_age_days        VARCHAR,
    days_since_last_activity VARCHAR,
    customer_age_days       VARCHAR,
    customer_tenure_days    VARCHAR,
    is_new_customer         VARCHAR,
    is_active_account       VARCHAR,
    is_active_customer      VARCHAR,
    is_withdrawal           VARCHAR,
    is_transfer             VARCHAR,
    is_purchase             VARCHAR,
    is_deposit              VARCHAR,
    is_atm                  VARCHAR,
    is_online               VARCHAR,
    is_mobile               VARCHAR,
    is_branch               VARCHAR,
    risk_score_base         VARCHAR,
    combined_risk_score     VARCHAR,
    transactions_last_1h    VARCHAR,
    transactions_last_24h   VARCHAR,
    amount_last_1h          VARCHAR,
    amount_last_24h         VARCHAR,
    avg_amount_24h          VARCHAR,
    processing_timestamp    VARCHAR,
    -- ETL metadata (populated by COPY INTO METADATA$)
    _LOAD_TS                TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE            VARCHAR
) DATA_RETENTION_TIME_IN_DAYS = 7;

CREATE OR REPLACE TABLE STAGING.RAW_REALTIME_FRAUD_DETECTION (
    TransactionID           VARCHAR,
    fraud_score             VARCHAR,
    is_fraud_predicted      VARCHAR,
    confidence_score        VARCHAR,
    fraud_type              VARCHAR,
    risk_indication         VARCHAR,
    action_taken            VARCHAR,
    inference_latency_ms    VARCHAR,
    processing_timestamp    VARCHAR,
    model_name              VARCHAR,
    model_version           VARCHAR,
    is_review_required      VARCHAR,
    -- ETL metadata
    _LOAD_TS                TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE            VARCHAR
) DATA_RETENTION_TIME_IN_DAYS = 7;


-- =============================================================================
--  5.  STAGING TABLE FOR BATCH TRANSACTION DATA
-- =============================================================================

CREATE OR REPLACE TABLE STAGING.RAW_TRANSACTION (
    TransactionID           VARCHAR,
    AccountNumber           VARCHAR,
    TransactionDate         VARCHAR,
    Amount                  VARCHAR,
    Currency                VARCHAR,
    TransactionTypeID       VARCHAR,
    TransactionMethod       VARCHAR,
    Channel                 VARCHAR,
    TransactionStatus       VARCHAR,
    MerchantID              VARCHAR,
    ATMID                   VARCHAR,
    City                    VARCHAR,
    State                   VARCHAR,
    Country                 VARCHAR,
    Latitude                VARCHAR,
    Longitude               VARCHAR,
    IsInternational         VARCHAR,
    DeviceID                VARCHAR,
    DeviceType              VARCHAR,
    IPAddress               VARCHAR,
    CardNumber              VARCHAR,
    -- ETL metadata
    _BATCH_ID               VARCHAR(100)    NOT NULL,
    _SOURCE_SYSTEM          VARCHAR(100)    NOT NULL,
    _LOAD_TS                TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _ROW_HASH               VARCHAR(64)     NOT NULL
) DATA_RETENTION_TIME_IN_DAYS = 7;


-- =============================================================================
--  6.  SAMPLE COPY INTO STATEMENTS
--      These are executed by the Airflow s3_to_snowflake.py task.
--      Listed here for reference / manual testing.
-- =============================================================================

-- Load real-time transactions from S3
-- COPY INTO STAGING.RAW_REALTIME_TRANSACTION
-- FROM @STAGING.S3_REALTIME_TXN_STAGE
-- FILE_FORMAT = STAGING.PARQUET_FORMAT
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
-- PATTERN = '.*\.parquet'
-- ON_ERROR = 'CONTINUE';

-- Load real-time fraud detection from S3
-- COPY INTO STAGING.RAW_REALTIME_FRAUD_DETECTION
-- FROM @STAGING.S3_REALTIME_DET_STAGE
-- FILE_FORMAT = STAGING.PARQUET_FORMAT
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
-- PATTERN = '.*\.parquet'
-- ON_ERROR = 'CONTINUE';

-- Load batch transaction data from S3
-- COPY INTO STAGING.RAW_TRANSACTION
-- FROM @STAGING.S3_BATCH_STAGE/transaction/
-- FILE_FORMAT = STAGING.PARQUET_FORMAT
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
-- PATTERN = '.*\.parquet'
-- ON_ERROR = 'CONTINUE';

