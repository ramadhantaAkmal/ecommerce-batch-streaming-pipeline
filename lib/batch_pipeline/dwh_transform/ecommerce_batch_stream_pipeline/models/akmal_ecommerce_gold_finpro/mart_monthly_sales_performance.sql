{{ config(
    materialized = 'table',
    partition_by = {
        "field": "order_month",
        "data_type": "date" 
    },
    cluster_by = ["year", "country_code"], 
    alias = 'mart_monthly_sales_performance'
) }}

WITH fact AS (
    SELECT
        date_id,
        order_id,
        user_id,
        amount_numeric,
        quantity,
        status,           
        country_code
    FROM {{ source('akmal_ecommerce_silver_finpro','fact_order') }}
),

daily AS (
    -- Reuse the daily mart logic to avoid duplication
    SELECT
        d.date      AS order_date,
        d.year,
        d.month,
        DATE_TRUNC(d.date, MONTH) AS order_month,
        f.country_code,
        COUNT(DISTINCT f.order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN f.status = 'genuine' THEN f.order_id END) AS genuine_orders,
        COUNT(DISTINCT CASE WHEN f.status = 'frauds'   THEN f.order_id END) AS fraud_orders,
        SUM(f.amount_numeric) AS gross_gmv,
        SUM(CASE WHEN f.status = 'genuine' THEN f.amount_numeric ELSE 0 END) AS net_revenue,
        SUM(CASE WHEN f.status = 'genuine' THEN f.quantity ELSE 0 END) AS genuine_units_sold
    FROM fact f
    LEFT JOIN {{ source('akmal_ecommerce_silver_finpro','dim_date') }} d ON f.date_id = d.date_id
    GROUP BY 1,2,3,4,5
)

SELECT
    order_month,
    year,
    month,
    FORMAT_DATE('%B', order_month)              AS month_name,
    FORMAT_DATE('%Y-%m', order_month)           AS year_month_label,

    -- Volume
    SUM(total_orders)                           AS total_orders,
    SUM(genuine_orders)                         AS genuine_orders,
    SUM(fraud_orders)                           AS fraud_orders,

    -- Revenue
    SUM(gross_gmv)                              AS gross_gmv,
    SUM(net_revenue)                            AS net_revenue,
    SUM(genuine_units_sold)                     AS genuine_units_sold,

    -- Key ratios
    SAFE_DIVIDE(SUM(net_revenue), SUM(genuine_orders)) 
                                                AS aov_genuine,

    SAFE_DIVIDE(SUM(genuine_units_sold), SUM(genuine_orders)) 
                                                AS avg_items_per_genuine_order,

    -- Rates
    SAFE_DIVIDE(SUM(fraud_orders), SUM(total_orders)) 
                                                AS fraud_rate,
    SAFE_DIVIDE(SUM(genuine_orders), SUM(total_orders)) 
                                                AS genuine_rate,

    -- Growth placeholders (will auto-calculate in BI tools or via window functions)
    LAG(SUM(net_revenue)) OVER (ORDER BY order_month) 
                                                AS net_revenue_previous_month,
    SAFE_DIVIDE(
        SUM(net_revenue) - LAG(SUM(net_revenue)) OVER (ORDER BY order_month),
        LAG(SUM(net_revenue)) OVER (ORDER BY order_month)
    )                                           AS mom_growth_rate

FROM daily
GROUP BY ALL
ORDER BY order_month DESC