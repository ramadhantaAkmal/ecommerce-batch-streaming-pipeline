{{ config(materialized='incremental', unique_key='user_id') }}

SELECT
    user_id,
    name,
    email,
    created_at AS customer_since
FROM {{ source('akmal_ecommerce_bronze_finpro', 'users') }}
{% if is_incremental() %}
WHERE created_at > (SELECT MAX(customer_since) FROM {{ this }})
{% endif %}