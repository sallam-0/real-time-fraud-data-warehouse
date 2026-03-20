-- dim_card.sql
-- DWH dimension: Card (Type 1 — overwrite on change)
-- PCI DSS: only SHA-256 hashed PAN stored, never raw digits.
-- Schema matches DDL: DWH.DIM_CARD

{{
    config(
        materialized='table',
        schema='DWH'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['card_number_hashed']) }} AS card_key,
    card_number_hashed,
    COALESCE(da.account_key, '-1')   AS account_key,
    c.card_type,
    c.card_network,
    c.issue_date,
    c.card_expiry_date,
    c.card_holder_name,
    c.daily_withdrawal_limit,
    c.daily_purchase_limit,
    c.single_txn_limit,
    c.card_status,
    CURRENT_TIMESTAMP()              AS etl_load_timestamp
FROM {{ ref('stg_card') }} c
LEFT JOIN {{ ref('dim_account') }} da
    ON c.account_number = da.account_number
    AND da.is_current = TRUE
