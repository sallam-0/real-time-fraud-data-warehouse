-- dim_transaction_type.sql
-- DWH dimension: Transaction Type (Type 1)

{{
    config(
        materialized='table',
        schema='DWH'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['transaction_type_id']) }} AS transaction_type_key,
    transaction_type_id,
    transaction_type_name,
    category,
    description,
    is_cash_transaction,
    is_cross_border,
    CURRENT_TIMESTAMP() AS etl_load_timestamp
FROM {{ ref('stg_transaction_type') }}
