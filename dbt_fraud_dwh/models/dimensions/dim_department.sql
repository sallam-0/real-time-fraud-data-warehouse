-- dim_department.sql
-- DWH dimension: Department (Type 1)
-- Schema matches DDL: DWH.DIM_DEPARTMENT
-- Note: manager_key uses raw manager_id to avoid circular dependency with dim_employee

{{
    config(
        materialized='table',
        schema='DWH'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['d.department_id']) }} AS department_key,
    d.department_id,
    d.department_name,
    d.manager_id                         AS manager_key,
    COALESCE(db.branch_key, '-1')        AS branch_key,
    CURRENT_TIMESTAMP()                  AS etl_load_timestamp
FROM {{ ref('stg_department') }} d
LEFT JOIN {{ ref('dim_branch') }} db
    ON d.branch_id = db.branch_id
