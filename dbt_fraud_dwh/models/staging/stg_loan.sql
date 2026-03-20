-- stg_loan.sql
-- Staging model: Clean and deduplicate RAW_LOAN

{{
    config(materialized='view')
}}

WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY LoanID ORDER BY _LOAD_TS DESC) AS rn
    FROM {{ source('staging', 'RAW_LOAN') }}
    WHERE _BATCH_ID = '{{ var("batch_id") }}'
)

SELECT
    TRIM(LoanID)                 AS loan_id,
    TRIM(CustomerID)             AS customer_id,
    TRIM(AccountNumber)          AS account_number,
    TRIM(BranchID)               AS branch_id,
    TRIM(LoanType)               AS loan_type,
    TRY_TO_NUMBER(LoanAmount, 18, 2)        AS loan_amount,
    TRY_TO_NUMBER(OutstandingAmount, 18, 2)  AS outstanding_amount,
    TRY_TO_NUMBER(InterestRate, 10, 4)       AS interest_rate,
    TRY_TO_NUMBER(LoanTermMonths)            AS loan_term_months,
    TRY_TO_DATE(StartDate, 'YYYY-MM-DD')     AS start_date,
    TRY_TO_DATE(EndDate, 'YYYY-MM-DD')       AS end_date,
    TRY_TO_DATE(NextPaymentDate, 'YYYY-MM-DD') AS next_payment_date,
    TRY_TO_NUMBER(MonthlyPayment, 18, 2)     AS monthly_payment,
    -- Derived fields
    CASE
        WHEN TRY_TO_NUMBER(OutstandingAmount, 18, 2) <= 0 THEN 'PAID_OFF'
        WHEN TRY_TO_DATE(EndDate, 'YYYY-MM-DD') < CURRENT_DATE() THEN 'OVERDUE'
        ELSE 'ACTIVE'
    END AS loan_status,
    CASE
        WHEN TRY_TO_NUMBER(LoanAmount, 18, 2) > 0
        THEN ROUND(
            (TRY_TO_NUMBER(LoanAmount, 18, 2) - TRY_TO_NUMBER(OutstandingAmount, 18, 2))
            / TRY_TO_NUMBER(LoanAmount, 18, 2) * 100, 2
        )
        ELSE 0
    END AS repayment_pct
FROM deduped
WHERE rn = 1
  AND NULLIF(TRIM(LoanID), '') IS NOT NULL
