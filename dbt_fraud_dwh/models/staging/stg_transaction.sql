-- stg_transaction.sql
-- Staging model: Clean and deduplicate RAW_TRANSACTION (batch MSSQL load)
-- Used by fact_loan_snapshot and fact_account_balance_snapshot

{{
    config(materialized='view')
}}

WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY TransactionID ORDER BY _LOAD_TS DESC) AS rn
    FROM {{ source('staging', 'RAW_TRANSACTION') }}
    WHERE _BATCH_ID = '{{ var("batch_id") }}'
)

SELECT
    TRIM(TransactionID)         AS transaction_id,
    TRIM(AccountNumber)         AS account_number,
    TRY_TO_TIMESTAMP(TransactionDate) AS transaction_date,
    TRY_TO_NUMBER(Amount, 18, 2)      AS amount,
    UPPER(TRIM(Currency))       AS currency,
    TRIM(TransactionTypeID)     AS transaction_type_id,
    TRIM(TransactionMethod)     AS transaction_method,
    TRIM(Channel)               AS channel,
    TRIM(MerchantID)            AS merchant_id,
    TRIM(ATMID)                 AS atm_id,
    TRIM(City)                  AS city,
    TRIM(State)                 AS state,
    UPPER(TRIM(Country))        AS country,
    TRY_TO_NUMBER(Latitude, 10, 7)  AS latitude,
    TRY_TO_NUMBER(Longitude, 10, 7) AS longitude,
    CASE WHEN UPPER(TRIM(IsInternational)) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_international,
    TRIM(DeviceID)              AS device_id,
    TRIM(DeviceType)            AS device_type,
    TRIM(IPAddress)             AS ip_address,
    SHA2(TRIM(CardNumber), 256) AS card_number_hashed,
    TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP(TransactionDate), 'YYYYMMDD')) AS date_key
FROM deduped
WHERE rn = 1
  AND NULLIF(TRIM(TransactionID), '') IS NOT NULL
