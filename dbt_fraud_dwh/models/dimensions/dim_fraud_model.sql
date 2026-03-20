-- dim_fraud_model.sql
-- DWH dimension: ML Fraud Model registry (Type 1 — DWH-only, no source equivalent)
-- Populated from distinct model_name + model_version seen in fraud detection output
-- Schema matches DDL: DWH.DIM_FRAUD_MODEL

{{
    config(
        materialized='table',
        schema='DWH'
    )
}}

WITH distinct_models AS (
    SELECT DISTINCT
        model_name,
        model_version
    FROM {{ ref('stg_realtime_fraud_detection') }}
    WHERE model_name IS NOT NULL
      AND model_version IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['model_name', 'model_version']) }} AS model_key,
    model_name,
    model_version,
    NULL::VARCHAR(100)          AS algorithm,
    NULL::VARCHAR(100)          AS framework,
    NULL::DATE                  AS training_date,
    NULL::DATE                  AS training_data_from,
    NULL::DATE                  AS training_data_to,
    NULL::DATE                  AS deploy_date,
    NULL::DATE                  AS retire_date,
    NULL::NUMBER(6,4)           AS threshold_score,
    NULL::NUMBER(6,4)           AS precision_score,
    NULL::NUMBER(6,4)           AS recall_score,
    NULL::NUMBER(6,4)           AS f1_score,
    NULL::NUMBER(6,4)           AS auc_roc,
    TRUE                        AS is_active,
    NULL::VARCHAR(2000)         AS feature_list,
    NULL::VARCHAR(1000)         AS description,
    CURRENT_TIMESTAMP()         AS etl_load_timestamp
FROM distinct_models
