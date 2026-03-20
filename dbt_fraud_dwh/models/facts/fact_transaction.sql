-- fact_transaction.sql
-- DWH fact: Transaction (incremental, deduped by transaction_id)
-- Sources: stg_realtime_transaction (Flink) + stg_transaction (batch MSSQL)
-- Schema matches DDL: DWH.FACT_TRANSACTION

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        schema='DWH',
        incremental_strategy='merge'
    )
}}

-- ── Real-time transactions (Flink CDC enriched) ────────────────────────
WITH realtime_txn AS (
    SELECT
        transaction_id,
        account_number,
        customer_id,
        merchant_id,
        atm_id,
        card_number_hashed,
        transaction_date,
        amount,
        currency,
        transaction_type_id,
        transaction_method,
        channel,
        city,
        state,
        country,
        latitude,
        longitude,
        is_international,
        transaction_hour,
        is_night_transaction,
        processing_timestamp AS load_ts
    FROM {{ ref('stg_realtime_transaction') }}

    {% if is_incremental() %}
    WHERE processing_timestamp > (
        SELECT COALESCE(MAX(etl_load_timestamp), '1970-01-01') FROM {{ this }}
    )
    {% endif %}
),

-- ── Batch transactions (MSSQL extract) — resolve customer_id via account join
batch_txn AS (
    SELECT
        t.transaction_id,
        t.account_number,
        a.customer_id,
        t.merchant_id,
        t.atm_id,
        t.card_number_hashed,
        t.transaction_date,
        t.amount,
        t.currency,
        t.transaction_type_id,
        t.transaction_method,
        t.channel,
        t.city,
        t.state,
        t.country,
        t.latitude,
        t.longitude,
        t.is_international,
        EXTRACT(HOUR FROM t.transaction_date)::NUMBER AS transaction_hour,
        CASE WHEN EXTRACT(HOUR FROM t.transaction_date) >= 22
              OR EXTRACT(HOUR FROM t.transaction_date) < 6
             THEN TRUE ELSE FALSE
        END                     AS is_night_transaction,
        t.transaction_date      AS load_ts
    FROM {{ ref('stg_transaction') }} t
    LEFT JOIN {{ ref('stg_account') }} a
        ON t.account_number = a.account_number

    {% if is_incremental() %}
    WHERE t.transaction_date > (
        SELECT COALESCE(MAX(etl_load_timestamp), '1970-01-01') FROM {{ this }}
    )
    {% endif %}
),

-- ── Combine both sources and deduplicate ───────────────────────────────
all_txn AS (
    SELECT *, 1 AS source_priority FROM realtime_txn
    UNION ALL
    SELECT *, 2 AS source_priority FROM batch_txn
),

txn AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY source_priority) AS rn
        FROM all_txn
    )
    WHERE rn = 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['txn.transaction_id']) }} AS transaction_fact_key,
    txn.transaction_id,

    -- Dimension foreign keys
    COALESCE(TRY_TO_NUMBER(TO_CHAR(txn.transaction_date, 'YYYYMMDD')), -1) AS date_key,
    COALESCE(dc.customer_key, '-1')          AS customer_key,
    COALESCE(da.account_key, '-1')           AS account_key,
    COALESCE(dcard.card_key, '-1')           AS card_key,
    COALESCE(dm.merchant_key, '-1')          AS merchant_key,
    COALESCE(datm.atm_key, '-1')             AS atm_key,
    COALESCE(da.branch_key, '-1')            AS branch_key,
    COALESCE(dtt.transaction_type_key, '-1') AS transaction_type_key,

    -- Measures — monetary
    txn.amount,
    txn.currency,

    -- Transaction attributes
    txn.transaction_method,
    txn.channel,
    txn.city,
    txn.state,
    txn.country,
    txn.latitude,
    txn.longitude,

    -- Derived behavioral features
    txn.transaction_hour            AS hour_of_day,
    txn.is_night_transaction,
    txn.is_international            AS is_cross_border,

    -- Ground truth fraud label (written back by analyst review pipeline)
    NULL::BOOLEAN                   AS is_fraud_confirmed,
    NULL::INT                       AS fraud_label_date_key,
    NULL::VARCHAR(50)               AS fraud_label_source,

    -- Timestamps
    txn.transaction_date            AS transaction_timestamp,
    CURRENT_TIMESTAMP()             AS etl_load_timestamp

FROM txn
LEFT JOIN {{ ref('dim_customer') }} dc
    ON txn.customer_id = dc.customer_id
    AND dc.is_current = TRUE
LEFT JOIN {{ ref('dim_account') }} da
    ON txn.account_number = da.account_number
    AND da.is_current = TRUE
LEFT JOIN {{ ref('dim_transaction_type') }} dtt
    ON txn.transaction_type_id = dtt.transaction_type_id
LEFT JOIN {{ ref('dim_merchant') }} dm
    ON txn.merchant_id = dm.merchant_id
LEFT JOIN {{ ref('dim_atm') }} datm
    ON txn.atm_id = datm.atm_id
LEFT JOIN {{ ref('dim_card') }} dcard
    ON txn.card_number_hashed = dcard.card_number_hashed
