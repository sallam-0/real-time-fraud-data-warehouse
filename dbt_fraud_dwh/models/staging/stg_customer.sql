-- stg_customer.sql
-- Staging model: Clean and deduplicate RAW_CUSTOMER

{{
    config(materialized='view')
}}

WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY _LOAD_TS DESC) AS rn
    FROM {{ source('staging', 'RAW_CUSTOMER') }}
    WHERE _BATCH_ID = '{{ var("batch_id") }}'
)

SELECT
    TRIM(CustomerID)            AS customer_id,
    TRIM(FirstName)             AS first_name,
    TRIM(LastName)              AS last_name,
    TRIM(FirstName) || ' ' || TRIM(LastName) AS full_name,
    NULLIF(TRIM(Email), '')     AS email,
    TRY_TO_DATE(DateOfBirth, 'YYYY-MM-DD') AS date_of_birth,
    TRIM(Gender)                AS gender,
    TRIM(MaritalStatus)         AS marital_status,
    TRIM(Occupation)            AS occupation,
    TRIM(StreetAddress)         AS street_address,
    TRIM(City)                  AS city,
    TRIM(State)                 AS state,
    UPPER(TRIM(Country))        AS country,
    TRIM(PostalCode)            AS postal_code,
    TRY_TO_DATE(CustomerSince, 'YYYY-MM-DD') AS customer_since,
    TRIM(CustomerSegment)       AS customer_segment,
    TRIM(IDType)                AS id_type,
    TRIM(IDNumber)              AS id_number,
    TRIM(TaxID)                 AS tax_id,
    -- SCD2 hash for tracking changes
    MD5(CONCAT_WS('|',
        IFNULL(TRIM(Occupation), ''),
        IFNULL(TRIM(StreetAddress), ''),
        IFNULL(TRIM(City), ''),
        IFNULL(TRIM(State), ''),
        IFNULL(TRIM(Country), ''),
        IFNULL(TRIM(CustomerSegment), '')
    )) AS scd2_hash
FROM deduped
WHERE rn = 1
  AND NULLIF(TRIM(CustomerID), '') IS NOT NULL
