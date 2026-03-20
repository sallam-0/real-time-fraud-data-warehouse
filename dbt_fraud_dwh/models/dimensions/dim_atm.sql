-- dim_atm.sql
-- DWH dimension: ATM (Type 1)
-- Schema matches DDL: DWH.DIM_ATM

{{
    config(
        materialized='table',
        schema='DWH'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['a.atm_id']) }} AS atm_key,
    a.atm_id,
    a.location                       AS location_name,
    a.street_address,
    a.city,
    a.state,
    a.country,
    a.postal_code,
    a.latitude,
    a.longitude,
    COALESCE(db.branch_key, '-1')    AS branch_key,
    a.atm_type,
    a.manufacturer,
    a.model,
    a.max_withdrawal_amount,
    a.atm_status,
    CURRENT_TIMESTAMP()              AS etl_load_timestamp
FROM {{ ref('stg_atm') }} a
LEFT JOIN {{ ref('dim_branch') }} db
    ON a.branch_id = db.branch_id
