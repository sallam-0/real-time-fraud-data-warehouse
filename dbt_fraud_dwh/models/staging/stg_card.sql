-- stg_card.sql
-- Staging model: Clean and deduplicate RAW_CARD
-- Hashes CardNumber with SHA-256 for PCI DSS compliance

{{
    config(materialized='view')
}}

WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY CardNumber ORDER BY _LOAD_TS DESC) AS rn
    FROM {{ source('staging', 'RAW_CARD') }}
    WHERE _BATCH_ID = '{{ var("batch_id") }}'
)

SELECT
    SHA2(TRIM(CardNumber), 256)          AS card_number_hashed,
    TRIM(AccountNumber)                  AS account_number,
    TRIM(CardType)                       AS card_type,
    TRIM(CardNetwork)                    AS card_network,
    TRY_TO_DATE(IssueDate)              AS issue_date,
    TRY_TO_DATE(ExpiryDate)             AS card_expiry_date,
    TRIM(CardHolderName)                AS card_holder_name,
    TRY_TO_NUMBER(DailyWithdrawalLimit, 18, 2)   AS daily_withdrawal_limit,
    TRY_TO_NUMBER(DailyPurchaseLimit, 18, 2)     AS daily_purchase_limit,
    TRY_TO_NUMBER(SingleTransactionLimit, 18, 2) AS single_txn_limit,
    TRIM(CardStatus)                    AS card_status
FROM deduped
WHERE rn = 1
  AND NULLIF(TRIM(CardNumber), '') IS NOT NULL
