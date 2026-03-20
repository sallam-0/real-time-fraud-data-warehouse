-- fact_loan_snapshot.sql
-- DWH fact: Monthly periodic snapshot of loan balances
-- Schema matches DDL: DWH.FACT_LOAN_SNAPSHOT

{{
    config(
        materialized='incremental',
        unique_key='snapshot_key',
        schema='DWH',
        incremental_strategy='merge'
    )
}}

WITH loans AS (
    SELECT * FROM {{ ref('stg_loan') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['l.loan_id', 'CURRENT_DATE()']) }} AS snapshot_key,
    l.loan_id,

    -- Dimension foreign keys
    COALESCE(dc.customer_key, '-1')  AS customer_key,
    COALESCE(db.branch_key, '-1')    AS branch_key,
    TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'))       AS snapshot_date_key,
    TRY_TO_NUMBER(TO_CHAR(l.start_date, 'YYYYMMDD'))     AS start_date_key,
    TRY_TO_NUMBER(TO_CHAR(l.end_date, 'YYYYMMDD'))       AS end_date_key,
    TRY_TO_NUMBER(TO_CHAR(l.next_payment_date, 'YYYYMMDD')) AS next_payment_date_key,

    -- Loan attributes
    l.loan_type,
    l.account_number,

    -- Measures
    l.loan_amount,
    l.outstanding_amount,
    l.interest_rate,
    l.loan_term_months,
    CASE
        WHEN l.end_date IS NOT NULL AND l.start_date IS NOT NULL
        THEN GREATEST(0, CEIL(DATEDIFF('month', CURRENT_DATE(), l.end_date)))
        ELSE NULL
    END                              AS remaining_months,
    l.monthly_payment,
    0                                AS days_past_due,
    FALSE                            AS is_delinquent,
    CURRENT_TIMESTAMP()              AS etl_load_timestamp

FROM loans l
LEFT JOIN {{ ref('dim_customer') }} dc
    ON l.customer_id = dc.customer_id AND dc.is_current = TRUE
LEFT JOIN {{ ref('dim_branch') }} db
    ON l.branch_id = db.branch_id