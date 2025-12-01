{{ config(
    materialized = 'table',
    partition_by = {
        "field": "order_month",
        "data_type": "date" 
    },
    cluster_by = ["payment_method", "year", "month"],
    alias = 'mart_payment_method_analysis'
) }}

WITH fact AS (
    SELECT
        date_id,
        order_id,
        amount_numeric,
        quantity,
        status,                
        payment_method,
        card_brand,
        ewallet_provider,
        country_code
    FROM {{ source('akmal_ecommerce_silver_finpro','fact_order') }}
),

date_dim AS (
    SELECT
        date_id,
        date               AS order_date,
        year,
        month,
        DATE_TRUNC(date, MONTH) AS order_month
    FROM {{ source('akmal_ecommerce_silver_finpro','dim_date') }}
)

SELECT
    -- Time
    d.order_month,
    d.year,
    d.month,
    FORMAT_DATE('%Y-%m', d.order_month) AS year_month,

    -- Payment dimensions
    f.payment_method,
    f.card_brand,
    f.ewallet_provider,

    -- Core counts
    COUNT(DISTINCT f.order_id)                                          AS total_orders,

    COUNT(DISTINCT CASE WHEN f.status = 'genuine' THEN f.order_id END)  AS genuine_orders,
    COUNT(DISTINCT CASE WHEN f.status = 'FRAUD'   THEN f.order_id END)  AS fraud_orders,

    -- Revenue & volume
    SUM(f.amount_numeric)                                               AS gross_gmv,
    SUM(CASE WHEN f.status = 'genuine' THEN f.amount_numeric ELSE 0 END) 
                                                                        AS net_revenue,

    SUM(CASE WHEN f.status = 'genuine' THEN f.quantity ELSE 0 END)      AS genuine_units,

    -- Key ratios
    SAFE_DIVIDE(
        SUM(CASE WHEN f.status = 'genuine' THEN f.amount_numeric ELSE 0 END),
        COUNT(DISTINCT CASE WHEN f.status = 'genuine' THEN f.order_id END)
    )                                                                   AS aov_genuine,

    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN f.status = 'FRAUD' THEN f.order_id END),
        COUNT(DISTINCT f.order_id)
    )                                                                   AS fraud_rate,

    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN f.status = 'genuine' THEN f.order_id END),
        COUNT(DISTINCT f.order_id)
    )                                                                   AS genuine_rate,

    -- Share of total
    SAFE_DIVIDE(SUM(CASE WHEN f.status = 'genuine' THEN f.amount_numeric ELSE 0 END),
                SUM(SUM(CASE WHEN f.status = 'genuine' THEN f.amount_numeric ELSE 0 END)) OVER (PARTITION BY d.order_month))
                                                                        AS revenue_share_of_total

FROM fact f
LEFT JOIN date_dim d USING (date_id)
GROUP BY ALL
ORDER BY order_month DESC, net_revenue DESC