{{ config(materialized='incremental', 
    unique_key='order_id',
    partition_by={
        "field": "DATE(last_updated)",
        "data_type": "date",
        "granularity": "day"
    },
) }}

WITH orders_raw AS (
    SELECT *
    FROM {{ source('akmal_ecommerce_bronze_finpro', 'orders') }}
)
    SELECT
        o.order_id,
        o.user_id,
        o.product_id,
        d.date_id,
        o.quantity,
        o.amount_numeric,
        o.country_code,
        o.payment_method,
        COALESCE(o.card_brand, 'NONE') as card_brand,
        COALESCE(o.card_country,'NONE') as card_country,
        COALESCE(o.bank_code,'NONE') as bank_code,
        COALESCE(o.ewallet_provider,'NONE') as ewallet_provider,
        COALESCE(o.billing_address,'NONE') as billing_address,
        o.status,
        o.fraud_reasons,
        EXTRACT(HOUR FROM o.created_at) AS created_hour,
        o.created_at as last_updated

    FROM orders_raw o
    LEFT JOIN {{ ref('dim_date') }} d
        ON DATE(o.created_at) = d.date
    {% if is_incremental() %}
    WHERE o.created_at > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}