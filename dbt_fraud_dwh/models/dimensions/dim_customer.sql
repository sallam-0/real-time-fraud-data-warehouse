-- dim_customer.sql
-- DWH dimension: Customer (SCD Type 2 — tracks address, segment, class changes)
-- Schema matches DDL: DWH.DIM_CUSTOMER

{{
    config(
        materialized='incremental',
        unique_key='customer_key',
        schema='DWH'
    )
}}

WITH staged AS (
    SELECT * FROM {{ ref('stg_customer') }}
),

{% if is_incremental() %}
existing AS (
    SELECT * FROM {{ this }}
    WHERE is_current = TRUE
),

changes AS (
    SELECT s.*
    FROM staged s
    LEFT JOIN existing e ON s.customer_id = e.customer_id
    WHERE e.customer_id IS NULL
       OR e.scd2_hash != s.scd2_hash
)
{% else %}
changes AS (
    SELECT * FROM staged
)
{% endif %}

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'scd2_hash']) }} AS customer_key,
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    date_of_birth,
    CASE
        WHEN date_of_birth IS NULL THEN 'Unknown'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 18 THEN 'Under 18'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) <= 25 THEN '18-25'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) <= 35 THEN '26-35'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) <= 45 THEN '36-45'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) <= 55 THEN '46-55'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) <= 65 THEN '56-65'
        ELSE '65+'
    END AS age_band,
    gender,
    marital_status,
    occupation,
    street_address,
    city,
    state,
    country,
    postal_code,
    customer_segment,
    id_type                     AS ic_type,
    id_number,
    NULL::VARCHAR(30)           AS primary_phone,
    -- SCD2 fields
    CURRENT_DATE()              AS effective_date,
    NULL::DATE                  AS expiry_date,
    TRUE                        AS is_current,
    -- Audit
    CURRENT_TIMESTAMP()         AS etl_load_timestamp
FROM changes
