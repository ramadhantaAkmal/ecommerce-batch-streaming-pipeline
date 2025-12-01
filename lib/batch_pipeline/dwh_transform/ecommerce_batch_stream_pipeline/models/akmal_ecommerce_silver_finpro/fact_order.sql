{{ config(materialized='incremental', 
    unique_key='order_id',
    description='Fact table for sales data in the Star Schema, partitioned by date',
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
    LEFT JOIN {{ ref('dim_users') }} c        ON o.user_id = c.user_id 
    LEFT JOIN {{ ref('dim_products') }} p         ON o.product_id = p.product_id
    LEFT JOIN {{ ref('dim_date') }} d            ON DATE(o.created_at) = d.date
    LEFT JOIN {{ ref('dim_payment_method') }} pm ON o.payment_method = pm.payment_method
                                              AND COALESCE(o.card_brand, '') = COALESCE(pm.card_brand, '')
    {% if is_incremental() %}
    WHERE o.created_at > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}