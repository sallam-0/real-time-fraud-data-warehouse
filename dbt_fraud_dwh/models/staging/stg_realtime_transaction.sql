-- stg_realtime_transaction.sql
-- Staging model: Clean real-time transaction data loaded from Flink S3 output
-- Source: Flink → S3 (Parquet) → Snowflake COPY INTO → RAW_REALTIME_TRANSACTION

{{
    config(materialized='view')
}}

SELECT
    TRIM(TransactionID)             AS transaction_id,
    TRIM(AccountNumber)             AS account_number,
    TRIM(CustomerID)                AS customer_id,
    TRIM(MerchantID)                AS merchant_id,
    TRIM(ATMID)                     AS atm_id,
    TRIM(BranchID)                  AS branch_id,
    TRIM(CardNumberHashed)          AS card_number_hashed,
    TRY_TO_TIMESTAMP(TransactionDate) AS transaction_date,
    TRY_TO_NUMBER(Amount, 18, 2)    AS amount,
    UPPER(TRIM(Currency))           AS currency,
    TRIM(TransactionTypeID)         AS transaction_type_id,
    TRIM(TransactionMethod)         AS transaction_method,
    TRIM(Channel)                   AS channel,
    TRIM(TransactionStatus)         AS transaction_status,
    -- Location
    TRIM(City)                      AS city,
    TRIM(State)                     AS state,
    UPPER(TRIM(Country))            AS country,
    TRY_TO_NUMBER(Latitude, 10, 7)  AS latitude,
    TRY_TO_NUMBER(Longitude, 10, 7) AS longitude,
    CASE WHEN UPPER(TRIM(IsInternational)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_international,
    -- Device
    TRIM(DeviceID)                  AS device_id,
    TRIM(DeviceType)                AS device_type,
    TRIM(IPAddress)                 AS ip_address,
    -- Customer enrichment
    TRIM(CustomerSegment)           AS customer_segment,
    TRIM(CustomerCity)              AS customer_city,
    UPPER(TRIM(CustomerCountry))    AS customer_country,
    -- Account enrichment
    TRIM(AccountType)               AS account_type,
    TRIM(AccountStatus)             AS account_status,
    TRY_TO_NUMBER(CurrentBalance, 18, 2)    AS current_balance,
    TRY_TO_NUMBER(AvailableBalance, 18, 2)  AS available_balance,
    -- Feature engineering fields
    TRY_TO_NUMBER(transaction_hour)         AS transaction_hour,
    TRY_TO_NUMBER(transaction_day_of_week)  AS transaction_day_of_week,
    CASE WHEN UPPER(TRIM(is_weekend)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN UPPER(TRIM(is_night_transaction)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_night_transaction,
    TRY_TO_DOUBLE(amount_to_balance_ratio)  AS amount_to_balance_ratio,
    TRY_TO_DOUBLE(amount_to_available_ratio) AS amount_to_available_ratio,
    CASE WHEN UPPER(TRIM(is_high_value)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_high_value,
    TRY_TO_DOUBLE(balance_utilization)      AS balance_utilization,
    CASE WHEN UPPER(TRIM(below_minimum_balance)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS below_minimum_balance,
    CASE WHEN UPPER(TRIM(is_home_city)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_home_city,
    CASE WHEN UPPER(TRIM(is_home_country)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_home_country,
    CASE WHEN UPPER(TRIM(location_mismatch)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS location_mismatch,
    CASE WHEN UPPER(TRIM(is_new_account)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_new_account,
    CASE WHEN UPPER(TRIM(is_dormant_account)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_dormant_account,
    TRY_TO_NUMBER(account_age_days)         AS account_age_days,
    TRY_TO_NUMBER(days_since_last_activity) AS days_since_last_activity,
    TRY_TO_NUMBER(customer_age_days)        AS customer_age_days,
    TRY_TO_NUMBER(customer_tenure_days)     AS customer_tenure_days,
    CASE WHEN UPPER(TRIM(is_new_customer)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_new_customer,
    CASE WHEN UPPER(TRIM(is_active_account)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_active_account,
    CASE WHEN UPPER(TRIM(is_active_customer)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_active_customer,
    -- Transaction type flags
    CASE WHEN UPPER(TRIM(is_withdrawal)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_withdrawal,
    CASE WHEN UPPER(TRIM(is_transfer)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_transfer,
    CASE WHEN UPPER(TRIM(is_purchase)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_purchase,
    CASE WHEN UPPER(TRIM(is_deposit)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_deposit,
    -- Channel flags
    CASE WHEN UPPER(TRIM(is_atm)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_atm,
    CASE WHEN UPPER(TRIM(is_online)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_online,
    CASE WHEN UPPER(TRIM(is_mobile)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_mobile,
    CASE WHEN UPPER(TRIM(is_branch)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_branch,
    -- Risk scoring
    TRY_TO_DOUBLE(risk_score_base)          AS risk_score_base,
    TRY_TO_DOUBLE(combined_risk_score)      AS combined_risk_score,
    -- Velocity features
    TRY_TO_NUMBER(transactions_last_1h)     AS transactions_last_1h,
    TRY_TO_NUMBER(transactions_last_24h)    AS transactions_last_24h,
    TRY_TO_DOUBLE(amount_last_1h)           AS amount_last_1h,
    TRY_TO_DOUBLE(amount_last_24h)          AS amount_last_24h,
    TRY_TO_DOUBLE(avg_amount_24h)           AS avg_amount_24h,
    -- Metadata
    TRY_TO_TIMESTAMP(processing_timestamp)  AS processing_timestamp,
    _LOAD_TS                                AS load_timestamp,
    _SOURCE_FILE                            AS source_file
FROM {{ source('staging', 'RAW_REALTIME_TRANSACTION') }}
WHERE NULLIF(TRIM(TransactionID), '') IS NOT NULL
