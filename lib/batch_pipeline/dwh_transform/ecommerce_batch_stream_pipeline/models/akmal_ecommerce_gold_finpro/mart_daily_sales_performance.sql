{{ config(
    materialized = 'table',
    partition_by = {
        "field": "order_date",
        "data_type": "date"
    },
    cluster_by = ["year", "month"],
    alias = 'mart_daily_sales_performance'
) }}

WITH fact AS (
    SELECT
        date_id,
        order_id,
        user_id,
        amount_numeric,
        quantity,
        status,           
        fraud_reasons,
        payment_method,
        country_code
    FROM {{ source('akmal_ecommerce_silver_finpro','fact_order') }}
),

date_dim AS (
    SELECT 
        date_id, 
        date AS order_date, 
        year, 
        month, 
        quarter, 
        day_of_week
    FROM {{ source('akmal_ecommerce_silver_finpro','dim_date') }}
)

SELECT
    -- Date
    d.order_date,
    d.year,
    d.month,
    d.quarter,
    d.day_of_week,
    EXTRACT(DAYOFWEEK FROM d.order_date) AS day_of_week_number,  -- 1=Sun, 7=Sat

    -- Core counts
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN f.status = 'genuine' THEN f.order_id END) AS genuine_orders,
    COUNT(DISTINCT CASE WHEN f.status = 'frauds'   THEN f.order_id END) AS fraud_orders,

    -- Revenue & GMV
    SUM(f.amount_numeric) AS gross_gmv,
    SUM(CASE WHEN f.status = 'genuine' THEN f.amount_numeric ELSE 0 END) AS net_revenue,   -- only genuine orders count as revenue
    SUM(CASE WHEN f.status = 'genuine' THEN f.quantity ELSE 0 END) AS genuine_units_sold,

    -- Key ratios
    SAFE_DIVIDE(
        SUM(CASE WHEN f.status = 'genuine' THEN f.amount_numeric ELSE 0 END),
        COUNT(DISTINCT CASE WHEN f.status = 'genuine' THEN f.order_id END)) AS aov_genuine,

    SAFE_DIVIDE(SUM(CASE WHEN f.status = 'genuine' THEN f.quantity ELSE 0 END),
                COUNT(DISTINCT CASE WHEN f.status = 'genuine' THEN f.order_id END)) AS avg_items_per_genuine_order,

    -- Fraud metrics
    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN f.status = 'frauds' THEN f.order_id END),
        COUNT(DISTINCT f.order_id)
    ) AS fraud_rate,

    -- Success rate
    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN f.status = 'genuine' THEN f.order_id END),
        COUNT(DISTINCT f.order_id)
    ) AS genuine_rate

FROM fact f
LEFT JOIN date_dim d USING (date_id)
GROUP BY ALL