-- dim_branch.sql
-- DWH dimension: Branch (Type 1 — overwrite on change)

{{
    config(
        materialized='table',
        schema='DWH'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['branch_id']) }} AS branch_key,
    branch_id,
    branch_name,
    branch_location,
    city,
    state,
    country,
    postal_code,
    open_date,
    branch_type,
    CURRENT_TIMESTAMP() AS etl_load_timestamp
FROM {{ ref('stg_branch') }}
