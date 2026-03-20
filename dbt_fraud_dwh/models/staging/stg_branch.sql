-- stg_branch.sql
-- Staging model: Clean and deduplicate RAW_BRANCH

{{
    config(materialized='view')
}}

WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY BranchID ORDER BY _LOAD_TS DESC) AS rn
    FROM {{ source('staging', 'RAW_BRANCH') }}
    WHERE _BATCH_ID = '{{ var("batch_id") }}'
)

SELECT
    TRIM(BranchID)          AS branch_id,
    TRIM(BranchName)        AS branch_name,
    TRIM(BranchLocation)    AS branch_location,
    TRIM(City)              AS city,
    TRIM(State)             AS state,
    UPPER(TRIM(Country))    AS country,
    TRIM(PostalCode)        AS postal_code,
    TRY_TO_DATE(OpenDate, 'YYYY-MM-DD') AS open_date,
    TRIM(BranchType)        AS branch_type
FROM deduped
WHERE rn = 1
  AND NULLIF(TRIM(BranchID), '') IS NOT NULL
