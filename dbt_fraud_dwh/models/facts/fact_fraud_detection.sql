-- fact_fraud_detection.sql
-- DWH fact: Fraud Detection results (incremental, deduped by transaction_id)
-- Source: stg_realtime_fraud_detection (Flink ML → S3 → Snowflake)
-- Schema matches DDL: DWH.FACT_FRAUD_DETECTION

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        schema='DWH',
        incremental_strategy='merge'
    )
}}

WITH raw_det AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY processing_timestamp DESC) AS rn
    FROM {{ ref('stg_realtime_fraud_detection') }}

    {% if is_incremental() %}
    WHERE processing_timestamp > (
        SELECT COALESCE(MAX(etl_load_timestamp), '1970-01-01') FROM {{ this }}
    )
    {% endif %}
),

det AS (
    SELECT * FROM raw_det WHERE rn = 1
)

SELECT
    -- Source business keys
    det.transaction_id,
    NULL::VARCHAR(50)            AS result_id,

    -- Dimension foreign keys
    COALESCE(ft.transaction_id, '-1')   AS transaction_fact_key,
    COALESCE(TRY_TO_NUMBER(TO_CHAR(det.processing_timestamp, 'YYYYMMDD')), -1) AS detection_date_key,
    COALESCE(ft.customer_key, '-1')     AS customer_key,
    COALESCE(ft.account_key, '-1')      AS account_key,
    COALESCE(dm.model_key, '-1')        AS model_key,

    -- ML output measures
    det.fraud_score,
    det.is_fraud_predicted,
    det.confidence_score,
    det.fraud_type,
    det.risk_indication,
    det.action_taken,
    det.is_review_required,

    -- Inference metadata
    det.inference_latency_ms,
    det.processing_timestamp            AS detection_timestamp,
    CURRENT_TIMESTAMP()                 AS etl_load_timestamp

FROM det
LEFT JOIN {{ ref('fact_transaction') }} ft
    ON det.transaction_id = ft.transaction_id
LEFT JOIN {{ ref('dim_fraud_model') }} dm
    ON det.model_name = dm.model_name
    AND det.model_version = dm.model_version
