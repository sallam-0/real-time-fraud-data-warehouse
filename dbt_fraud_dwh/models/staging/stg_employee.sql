-- stg_employee.sql
-- Staging model: Clean and deduplicate RAW_EMPLOYEE

{{
    config(materialized='view')
}}

WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY EmployeeID ORDER BY _LOAD_TS DESC) AS rn
    FROM {{ source('staging', 'RAW_EMPLOYEE') }}
    WHERE _BATCH_ID = '{{ var("batch_id") }}'
)

SELECT
    TRIM(EmployeeID)            AS employee_id,
    TRIM(FirstName)             AS first_name,
    TRIM(LastName)              AS last_name,
    TRIM(FirstName) || ' ' || TRIM(LastName) AS full_name,
    NULLIF(TRIM(Email), '')     AS email,
    NULLIF(TRIM(PhoneNumber), '') AS phone_number,
    TRY_TO_DATE(DateOfBirth, 'YYYY-MM-DD') AS date_of_birth,
    TRY_TO_DATE(HireDate, 'YYYY-MM-DD')   AS hire_date,
    NULLIF(TRIM(Position), '')  AS position,
    TRY_TO_NUMBER(Salary, 18, 2) AS salary,
    TRIM(DepartmentID)          AS department_id,
    TRIM(BranchID)              AS branch_id,
    TRIM(SupervisorID)          AS supervisor_id,
    -- SCD2 hash of tracked columns
    MD5(CONCAT_WS('|',
        IFNULL(TRIM(Position), ''),
        IFNULL(TRIM(Salary), ''),
        IFNULL(TRIM(DepartmentID), ''),
        IFNULL(TRIM(BranchID), '')
    )) AS scd2_hash
FROM deduped
WHERE rn = 1
  AND NULLIF(TRIM(EmployeeID), '') IS NOT NULL
