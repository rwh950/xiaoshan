-- 1. 数据总览
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT invoice_no) AS invoice_count,
    COUNT(DISTINCT stock_code) AS product_count,
    COUNT(DISTINCT country) AS country_count
FROM online_retail;


-- 2. 国家销售额 TOP 10
SELECT
    country,
    COUNT(DISTINCT invoice_no) AS invoice_count,
    SUM(quantity) AS quantity,
    ROUND(SUM(amount), 2) AS sales_amount
FROM online_retail
WHERE order_type = 'SALE'
GROUP BY country
ORDER BY sales_amount DESC
LIMIT 10;


-- 3. 商品销售额 TOP 10
SELECT
    stock_code,
    FIRST(description, TRUE) AS description,
    SUM(quantity) AS quantity,
    ROUND(SUM(amount), 2) AS sales_amount
FROM online_retail
WHERE order_type = 'SALE'
GROUP BY stock_code
ORDER BY sales_amount DESC
LIMIT 10;


-- 4. 月度销售
SELECT
    invoice_year,
    invoice_month,
    COUNT(DISTINCT invoice_no) AS invoice_count,
    SUM(quantity) AS quantity,
    ROUND(SUM(amount), 2) AS sales_amount
FROM online_retail
WHERE order_type = 'SALE'
GROUP BY invoice_year, invoice_month
ORDER BY invoice_year, invoice_month;
