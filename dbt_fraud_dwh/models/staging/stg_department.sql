-- stg_department.sql
-- Staging model: Clean and deduplicate RAW_DEPARTMENT

{{
    config(materialized='view')
}}

WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY DepartmentID ORDER BY _LOAD_TS DESC) AS rn
    FROM {{ source('staging', 'RAW_DEPARTMENT') }}
    WHERE _BATCH_ID = '{{ var("batch_id") }}'
)

SELECT
    TRIM(DepartmentID)      AS department_id,
    TRIM(DepartmentName)    AS department_name,
    TRIM(ManagerID)         AS manager_id,
    TRIM(BranchID)          AS branch_id
FROM deduped
WHERE rn = 1
  AND NULLIF(TRIM(DepartmentID), '') IS NOT NULL
