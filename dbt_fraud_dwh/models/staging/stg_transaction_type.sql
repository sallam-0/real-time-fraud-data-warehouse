-- stg_transaction_type.sql
-- Staging model: Clean and deduplicate RAW_TRANSACTION_TYPE

{{
    config(materialized='view')
}}

WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY TransactionTypeID ORDER BY _LOAD_TS DESC) AS rn
    FROM {{ source('staging', 'RAW_TRANSACTION_TYPE') }}
    WHERE _BATCH_ID = '{{ var("batch_id") }}'
)

SELECT
    TRIM(TransactionTypeID)     AS transaction_type_id,
    TRIM(TransactionTypeName)   AS transaction_type_name,
    TRIM(Category)              AS category,
    TRIM(Description)           AS description,
    CASE
        WHEN UPPER(TRIM(Category)) IN ('CASH', 'WITHDRAWAL', 'DEPOSIT')
        THEN TRUE ELSE FALSE
    END AS is_cash_transaction,
    CASE
        WHEN UPPER(TRIM(Category)) LIKE '%INTERNATIONAL%'
          OR UPPER(TRIM(Category)) LIKE '%CROSS%'
        THEN TRUE ELSE FALSE
    END AS is_cross_border
FROM deduped
WHERE rn = 1
  AND NULLIF(TRIM(TransactionTypeID), '') IS NOT NULL
