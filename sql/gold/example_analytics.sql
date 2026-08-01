-- NovaRetail certified Gold query examples.
-- Register the Delta paths as tables/views in local Spark SQL or Databricks first.

-- 1. Daily certified sales and average order value by channel.
SELECT
    d.full_date,
    c.channel_name,
    k.gross_sales,
    k.net_sales,
    k.net_revenue,
    k.average_order_value
FROM retail_kpi_daily AS k
JOIN dim_date AS d ON d.date_key = k.order_date_key
JOIN dim_channel AS c ON c.channel_key = k.channel_key
ORDER BY d.full_date, c.channel_name;

-- 2. Best-selling products by certified line revenue.
SELECT
    p.product_name,
    p.category,
    SUM(a.sold_units) AS units_sold,
    SUM(a.product_revenue) AS product_revenue,
    SUM(a.refund_amount) AS refunds
FROM agg_daily_product_sales AS a
JOIN dim_product AS p
    ON p.surrogate_key = a.product_key
GROUP BY p.product_name, p.category
ORDER BY product_revenue DESC
LIMIT 20;

-- 3. Store inventory health and stockout rate.
SELECT
    s.store_name,
    s.region,
    SUM(i.product_observations) AS product_observations,
    SUM(i.stockout_observations) AS stockouts,
    SUM(i.stockout_observations) / NULLIF(SUM(i.product_observations), 0) AS stockout_rate
FROM agg_inventory_health AS i
JOIN dim_store AS s ON s.surrogate_key = i.store_key
GROUP BY s.store_name, s.region
ORDER BY stockout_rate DESC;

-- 4. Digital conversion by date and device.
SELECT
    d.full_date,
    f.device_type,
    f.eligible_sessions,
    f.purchase_sessions,
    f.conversion_rate
FROM agg_channel_funnel AS f
JOIN dim_date AS d ON d.date_key = f.event_date_key
ORDER BY d.full_date, f.device_type;

-- 5. Promotion-attributed performance.
SELECT
    p.promotion_name,
    SUM(a.attributed_orders) AS attributed_orders,
    SUM(a.attributed_revenue) AS attributed_revenue,
    SUM(a.attributed_discounts) AS attributed_discounts
FROM agg_promotion_performance AS a
JOIN dim_promotion AS p ON p.promotion_key = a.promotion_key
GROUP BY p.promotion_name
ORDER BY attributed_revenue DESC;

-- 6. Supplier contribution to sold units and revenue.
SELECT
    s.supplier_name,
    SUM(a.units_sold) AS units_sold,
    SUM(a.supplier_revenue) AS supplier_revenue
FROM agg_supplier_performance AS a
JOIN dim_supplier AS s ON s.surrogate_key = a.supplier_key
GROUP BY s.supplier_name
ORDER BY supplier_revenue DESC;
