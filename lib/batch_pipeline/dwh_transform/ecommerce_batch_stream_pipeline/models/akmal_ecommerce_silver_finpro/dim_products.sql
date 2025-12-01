{{ config(materialized='incremental', unique_key='product_id') }}

SELECT
    product_id,
    product_name,
    category,
    price,
    created_at
FROM {{ source('akmal_ecommerce_bronze_finpro', 'products') }}
{% if is_incremental() %}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}