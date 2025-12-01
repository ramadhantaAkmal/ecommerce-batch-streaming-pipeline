{{ config(materialized='view') }}

SELECT DISTINCT
    payment_method,
    card_brand,
    card_country,
    bank_code,
    ewallet_provider
FROM {{ source('akmal_ecommerce_bronze_finpro', 'orders') }}
WHERE payment_method IS NOT NULL