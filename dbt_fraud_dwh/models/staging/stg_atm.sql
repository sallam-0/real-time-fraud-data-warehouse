-- stg_atm.sql
-- Staging model: Clean and deduplicate RAW_ATM

{{
    config(materialized='view')
}}

WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ATMID ORDER BY _LOAD_TS DESC) AS rn
    FROM {{ source('staging', 'RAW_ATM') }}
    WHERE _BATCH_ID = '{{ var("batch_id") }}'
)

SELECT
    TRIM(ATMID)                 AS atm_id,
    TRIM(Location)              AS location,
    TRIM(StreetAddress)         AS street_address,
    TRIM(City)                  AS city,
    TRIM(State)                 AS state,
    UPPER(TRIM(Country))        AS country,
    TRIM(PostalCode)            AS postal_code,
    TRY_TO_NUMBER(Latitude, 10, 7)  AS latitude,
    TRY_TO_NUMBER(Longitude, 10, 7) AS longitude,
    TRIM(BranchID)              AS branch_id,
    TRIM(ATMType)               AS atm_type,
    TRIM(Manufacturer)          AS manufacturer,
    TRIM(Model)                 AS model,
    TRY_TO_NUMBER(MaxWithdrawalAmount, 18, 2) AS max_withdrawal_amount,
    TRY_TO_NUMBER(CurrentCashLevel, 18, 2) AS current_cash_level,
    TRIM(ATMStatus)             AS atm_status,
    TRY_TO_DATE(InstallationDate, 'YYYY-MM-DD') AS installation_date,
    TRY_TO_DATE(LastMaintenanceDate, 'YYYY-MM-DD') AS last_maintenance_date
FROM deduped
WHERE rn = 1
  AND NULLIF(TRIM(ATMID), '') IS NOT NULL
