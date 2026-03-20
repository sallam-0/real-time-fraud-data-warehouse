-- dim_merchant.sql
-- DWH dimension: Merchant (Type 1)
-- Schema matches DDL: DWH.DIM_MERCHANT

{{
    config(
        materialized='table',
        schema='DWH'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['merchant_id']) }} AS merchant_key,
    merchant_id,
    merchant_name,
    merchant_category,
    mcc,
    street_address,
    city,
    state,
    country,
    postal_code,
    business_type,
    website_url,
    phone_number,
    is_high_risk,
    CURRENT_TIMESTAMP()         AS etl_load_timestamp
FROM {{ ref('stg_merchant') }}
