{{ config(
    materialized='table',
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY date) AS date_id,
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(QUARTER FROM date) AS quarter,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    FORMAT_DATE('%A', date) AS day_of_week
FROM (
    SELECT DISTINCT DATE(created_at) AS date
    FROM {{ source('akmal_ecommerce_bronze_finpro', 'orders') }}
) dates