-- dim_employee.sql
-- DWH dimension: Employee (SCD Type 2 — track position, salary, department changes)
-- Schema matches DDL: DWH.DIM_EMPLOYEE

{{
    config(
        materialized='incremental',
        unique_key='employee_key',
        schema='DWH'
    )
}}

WITH staged AS (
    SELECT * FROM {{ ref('stg_employee') }}
),

{% if is_incremental() %}
existing AS (
    SELECT * FROM {{ this }}
    WHERE is_current = TRUE
),

changes AS (
    SELECT s.*
    FROM staged s
    LEFT JOIN existing e ON s.employee_id = e.employee_id
    WHERE e.employee_id IS NULL
       OR e.scd2_hash != s.scd2_hash
)
{% else %}
changes AS (
    SELECT * FROM staged
)
{% endif %}

SELECT
    {{ dbt_utils.generate_surrogate_key(['c.employee_id', 'c.scd2_hash']) }} AS employee_key,
    c.employee_id,
    c.first_name,
    c.last_name,
    c.full_name,
    c.email,
    c.phone_number,
    c.date_of_birth,
    c.hire_date,
    c.position,
    c.salary,
    COALESCE(dd.department_key, '-1')   AS department_key,
    COALESCE(db.branch_key, '-1')       AS branch_key,
    c.supervisor_id                     AS supervisor_key,
    -- SCD2 fields
    CURRENT_DATE()                      AS effective_date,
    NULL::DATE                          AS expiry_date,
    TRUE                                AS is_current,
    -- Audit
    CURRENT_TIMESTAMP()                 AS etl_load_timestamp
FROM changes c
LEFT JOIN {{ ref('dim_department') }} dd
    ON c.department_id = dd.department_id
LEFT JOIN {{ ref('dim_branch') }} db
    ON c.branch_id = db.branch_id
