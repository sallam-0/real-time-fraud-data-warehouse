-- dim_account.sql
-- DWH dimension: Account (SCD Type 2 — tracks status, type, branch changes)
-- Schema matches DDL: DWH.DIM_ACCOUNT

{{
    config(
        materialized='incremental',
        unique_key='account_key',
        schema='DWH'
    )
}}

WITH staged AS (
    SELECT * FROM {{ ref('stg_account') }}
),

{% if is_incremental() %}
existing AS (
    SELECT * FROM {{ this }}
    WHERE is_current = TRUE
),

changes AS (
    SELECT s.*
    FROM staged s
    LEFT JOIN existing e ON s.account_number = e.account_number
    WHERE e.account_number IS NULL
       OR e.scd2_hash != s.scd2_hash
)
{% else %}
changes AS (
    SELECT * FROM staged
)
{% endif %}

SELECT
    {{ dbt_utils.generate_surrogate_key(['account_number', 'scd2_hash']) }} AS account_key,
    c.account_number,
    COALESCE(dc.customer_key, '-1')  AS customer_key,
    COALESCE(db.branch_key, '-1')    AS branch_key,
    c.account_type,
    c.currency,
    c.open_date,
    c.last_activity_date,
    c.interest_rate,
    c.minimum_balance,
    c.overdraft_limit,
    -- SCD2 fields
    CURRENT_DATE()                   AS effective_date,
    NULL::DATE                       AS expiry_date,
    TRUE                             AS is_current,
    -- Audit
    CURRENT_TIMESTAMP()              AS etl_load_timestamp
FROM changes c
LEFT JOIN {{ ref('dim_customer') }} dc
    ON c.customer_id = dc.customer_id
    AND dc.is_current = TRUE
LEFT JOIN {{ ref('dim_branch') }} db
    ON c.branch_id = db.branch_id
