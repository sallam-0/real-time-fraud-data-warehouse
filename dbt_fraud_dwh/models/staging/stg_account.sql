-- stg_account.sql
-- Staging model: Clean and deduplicate RAW_ACCOUNT

{{
    config(materialized='view')
}}

WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY AccountNumber ORDER BY _LOAD_TS DESC) AS rn
    FROM {{ source('staging', 'RAW_ACCOUNT') }}
    WHERE _BATCH_ID = '{{ var("batch_id") }}'
)

SELECT
    TRIM(AccountNumber)         AS account_number,
    TRIM(CustomerID)            AS customer_id,
    TRIM(AccountType)           AS account_type,
    TRIM(BranchID)              AS branch_id,
    TRY_TO_NUMBER(Balance, 18, 2)          AS balance,
    TRY_TO_NUMBER(AvailableBalance, 18, 2) AS available_balance,
    UPPER(TRIM(Currency))       AS currency,
    TRY_TO_DATE(OpenDate, 'YYYY-MM-DD')           AS open_date,
    TRY_TO_DATE(LastActivityDate, 'YYYY-MM-DD')    AS last_activity_date,
    TRY_TO_NUMBER(InterestRate, 10, 4)     AS interest_rate,
    TRY_TO_NUMBER(MinimumBalance, 18, 2)   AS minimum_balance,
    TRY_TO_NUMBER(OverdraftLimit, 18, 2)   AS overdraft_limit,
    TRIM(AccountStatus)         AS account_status,
    TRY_TO_TIMESTAMP(CreatedDate)  AS created_date,
    TRY_TO_TIMESTAMP(ModifiedDate) AS modified_date,
    -- SCD2 hash
    MD5(CONCAT_WS('|',
        IFNULL(TRIM(AccountType), ''),
        IFNULL(TRIM(AccountStatus), ''),
        IFNULL(TRIM(BranchID), ''),
        IFNULL(TRIM(Currency), '')
    )) AS scd2_hash
FROM deduped
WHERE rn = 1
  AND NULLIF(TRIM(AccountNumber), '') IS NOT NULL
