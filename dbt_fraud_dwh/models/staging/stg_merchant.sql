-- stg_merchant.sql
-- Staging model: Clean and deduplicate RAW_MERCHANT

{{
    config(materialized='view')
}}

WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY MerchantID ORDER BY _LOAD_TS DESC) AS rn
    FROM {{ source('staging', 'RAW_MERCHANT') }}
    WHERE _BATCH_ID = '{{ var("batch_id") }}'
)

SELECT
    TRIM(MerchantID)            AS merchant_id,
    TRIM(MerchantName)          AS merchant_name,
    TRIM(MerchantCategory)      AS merchant_category,
    TRIM(MCC)                   AS mcc,
    TRIM(StreetAddress)         AS street_address,
    TRIM(City)                  AS city,
    TRIM(State)                 AS state,
    UPPER(TRIM(Country))        AS country,
    TRIM(PostalCode)            AS postal_code,
    TRIM(PhoneNumber)           AS phone_number,
    TRIM(BusinessType)          AS business_type,
    TRIM(WebsiteURL)            AS website_url,
    CASE
        WHEN TRIM(MCC) IN ('7995','5912','5999','6211','7801','7802','7993')
        THEN TRUE ELSE FALSE
    END AS is_high_risk
FROM deduped
WHERE rn = 1
  AND NULLIF(TRIM(MerchantID), '') IS NOT NULL
