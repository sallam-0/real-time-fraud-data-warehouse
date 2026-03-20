-- fact_account_balance_snapshot.sql
-- DWH fact: Daily periodic snapshot of account balances
-- Schema matches DDL: DWH.FACT_ACCOUNT_BALANCE_SNAPSHOT

{{
    config(
        materialized='incremental',
        unique_key='snapshot_key',
        schema='DWH',
        incremental_strategy='merge'
    )
}}

WITH accounts AS (
    SELECT * FROM {{ ref('stg_account') }}
),

-- Aggregate batch transaction activity per account
account_activity AS (
    SELECT
        t.account_number,
        COUNT(*)                    AS txn_count,
        SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) AS total_credits,
        SUM(CASE WHEN t.amount < 0 THEN ABS(t.amount) ELSE 0 END) AS total_debits
    FROM {{ ref('stg_transaction') }} t
    GROUP BY t.account_number
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['a.account_number', 'CURRENT_DATE()']) }} AS snapshot_key,

    -- Dimension foreign keys
    COALESCE(da.account_key, '-1')  AS account_key,
    COALESCE(dc.customer_key, '-1') AS customer_key,
    TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')) AS snapshot_date_key,

    -- Measures
    a.balance                       AS balance,
    a.available_balance,
    a.currency,
    COALESCE(act.total_debits, 0)   AS daily_debit_total,
    COALESCE(act.total_credits, 0)  AS daily_credit_total,
    COALESCE(act.txn_count, 0)      AS transaction_count,

    CURRENT_TIMESTAMP()             AS etl_load_timestamp

FROM accounts a
LEFT JOIN {{ ref('dim_customer') }} dc
    ON a.customer_id = dc.customer_id AND dc.is_current = TRUE
LEFT JOIN {{ ref('dim_account') }} da
    ON a.account_number = da.account_number AND da.is_current = TRUE
LEFT JOIN account_activity act
    ON a.account_number = act.account_number
