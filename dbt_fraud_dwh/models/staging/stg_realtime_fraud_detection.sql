-- stg_realtime_fraud_detection.sql
-- Staging model: Clean real-time fraud detection data from Flink S3 output
-- Source: Flink ML scoring → S3 (Parquet) → Snowflake COPY INTO

{{
    config(materialized='view')
}}

SELECT
    TRIM(TransactionID)                     AS transaction_id,
    TRY_TO_DOUBLE(fraud_score)              AS fraud_score,
    CASE WHEN UPPER(TRIM(is_fraud_predicted)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_fraud_predicted,
    TRY_TO_DOUBLE(confidence_score)         AS confidence_score,
    TRIM(fraud_type)                        AS fraud_type,
    TRIM(risk_indication)                   AS risk_indication,
    TRIM(action_taken)                      AS action_taken,
    TRY_TO_DOUBLE(inference_latency_ms)     AS inference_latency_ms,
    TRY_TO_TIMESTAMP(processing_timestamp)  AS processing_timestamp,
    TRIM(model_name)                        AS model_name,
    TRIM(model_version)                     AS model_version,
    CASE WHEN UPPER(TRIM(is_review_required)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_review_required,
    -- Metadata
    _LOAD_TS                                AS load_timestamp,
    _SOURCE_FILE                            AS source_file
FROM {{ source('staging', 'RAW_REALTIME_FRAUD_DETECTION') }}
WHERE NULLIF(TRIM(TransactionID), '') IS NOT NULL
