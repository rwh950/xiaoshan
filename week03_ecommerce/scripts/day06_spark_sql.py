from pyspark.sql import SparkSession


# ============================================================
# 配置
# ============================================================

CLEAN_PATH = (
    "hdfs://master:9000/"
    "user/hadoop/week03/clean"
)


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("week04_day06_spark_sql")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 80)
print("Week04 Day06 - Spark SQL 数据分析")
print("=" * 80)

print("Spark Master:", spark.sparkContext.master)
print("Clean Path:", CLEAN_PATH)


# ============================================================
# 1. 读取 Parquet
# ============================================================

df = spark.read.parquet(CLEAN_PATH)

print()
print("=" * 80)
print("1. Parquet 数据概况")
print("=" * 80)

print("数据行数:", df.count())

print()
print("Schema:")
df.printSchema()


# ============================================================
# 2. 注册临时视图
# ============================================================

df.createOrReplaceTempView("online_retail")


# ============================================================
# 3. 数据总览
# ============================================================

print()
print("=" * 80)
print("2. 数据总览")
print("=" * 80)

spark.sql(
    """
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT invoice_no) AS invoice_count,
        COUNT(DISTINCT stock_code) AS product_count,
        COUNT(DISTINCT country) AS country_count
    FROM online_retail
    """
).show(truncate=False)


# ============================================================
# 4. order_type 分布
# ============================================================

print()
print("=" * 80)
print("3. 业务类型分布")
print("=" * 80)

spark.sql(
    """
    SELECT
        order_type,
        COUNT(*) AS row_count,
        ROUND(SUM(amount), 2) AS amount_sum
    FROM online_retail
    GROUP BY order_type
    ORDER BY row_count DESC
    """
).show(truncate=False)


# ============================================================
# 5. 正常销售核心指标
#
# 这里只统计 SALE
# 防止把 ADJUSTMENT / ZERO_PRICE 算进正常销售额
# ============================================================

print()
print("=" * 80)
print("4. 正常销售核心指标")
print("=" * 80)

spark.sql(
    """
    SELECT
        COUNT(*) AS sale_rows,
        COUNT(DISTINCT invoice_no) AS sale_invoice_count,
        COUNT(DISTINCT customer_id) AS customer_count,
        COUNT(DISTINCT stock_code) AS product_count,
        SUM(quantity) AS sale_quantity,
        ROUND(SUM(amount), 2) AS total_sales_amount,
        ROUND(AVG(amount), 2) AS avg_sale_amount
    FROM online_retail
    WHERE order_type = 'SALE'
    """
).show(truncate=False)


# ============================================================
# 6. 国家销售排行 TOP 10
#
# 只统计正常销售
# ============================================================

print()
print("=" * 80)
print("5. 国家销售额 TOP 10")
print("=" * 80)

spark.sql(
    """
    SELECT
        country,
        COUNT(DISTINCT invoice_no) AS invoice_count,
        SUM(quantity) AS quantity,
        ROUND(SUM(amount), 2) AS sales_amount
    FROM online_retail
    WHERE order_type = 'SALE'
    GROUP BY country
    ORDER BY sales_amount DESC
    LIMIT 10
    """
).show(
    10,
    truncate=False
)


# ============================================================
# 7. 商品销售额 TOP 10
# ============================================================

print()
print("=" * 80)
print("6. 商品销售额 TOP 10")
print("=" * 80)

spark.sql(
    """
    SELECT
        stock_code,
        FIRST(description, TRUE) AS description,
        SUM(quantity) AS quantity,
        ROUND(SUM(amount), 2) AS sales_amount
    FROM online_retail
    WHERE order_type = 'SALE'
    GROUP BY stock_code
    ORDER BY sales_amount DESC
    LIMIT 10
    """
).show(
    10,
    truncate=False
)


# ============================================================
# 8. 商品销量 TOP 10
# ============================================================

print()
print("=" * 80)
print("7. 商品销量 TOP 10")
print("=" * 80)

spark.sql(
    """
    SELECT
        stock_code,
        FIRST(description, TRUE) AS description,
        SUM(quantity) AS quantity,
        ROUND(SUM(amount), 2) AS sales_amount
    FROM online_retail
    WHERE order_type = 'SALE'
    GROUP BY stock_code
    ORDER BY quantity DESC
    LIMIT 10
    """
).show(
    10,
    truncate=False
)


# ============================================================
# 9. 月度销售趋势
# ============================================================

print()
print("=" * 80)
print("8. 月度销售趋势")
print("=" * 80)

spark.sql(
    """
    SELECT
        invoice_year,
        invoice_month,
        COUNT(DISTINCT invoice_no) AS invoice_count,
        SUM(quantity) AS quantity,
        ROUND(SUM(amount), 2) AS sales_amount
    FROM online_retail
    WHERE order_type = 'SALE'
    GROUP BY
        invoice_year,
        invoice_month
    ORDER BY
        invoice_year,
        invoice_month
    """
).show(
    100,
    truncate=False
)


# ============================================================
# 10. 小时销售趋势
# ============================================================

print()
print("=" * 80)
print("9. 小时销售趋势")
print("=" * 80)

spark.sql(
    """
    SELECT
        invoice_hour,
        COUNT(DISTINCT invoice_no) AS invoice_count,
        SUM(quantity) AS quantity,
        ROUND(SUM(amount), 2) AS sales_amount
    FROM online_retail
    WHERE order_type = 'SALE'
    GROUP BY invoice_hour
    ORDER BY invoice_hour
    """
).show(
    24,
    truncate=False
)


# ============================================================
# 11. 用户销售额 TOP 10
#
# CustomerID 为空的不参与用户排行
# ============================================================

print()
print("=" * 80)
print("10. 用户销售额 TOP 10")
print("=" * 80)

spark.sql(
    """
    SELECT
        customer_id,
        COUNT(DISTINCT invoice_no) AS invoice_count,
        SUM(quantity) AS quantity,
        ROUND(SUM(amount), 2) AS sales_amount
    FROM online_retail
    WHERE
        order_type = 'SALE'
        AND customer_id IS NOT NULL
    GROUP BY customer_id
    ORDER BY sales_amount DESC
    LIMIT 10
    """
).show(
    10,
    truncate=False
)


# ============================================================
# 12. 退货统计
# ============================================================

print()
print("=" * 80)
print("11. 退货情况")
print("=" * 80)

spark.sql(
    """
    SELECT
        COUNT(*) AS return_rows,
        COUNT(DISTINCT invoice_no) AS return_invoice_count,
        SUM(quantity) AS return_quantity,
        ROUND(SUM(amount), 2) AS return_amount
    FROM online_retail
    WHERE order_type = 'RETURN'
    """
).show(truncate=False)


# ============================================================
# 13. 净交易金额
#
# SALE + RETURN
# 排除 ADJUSTMENT 和 ZERO_PRICE
# ============================================================

print()
print("=" * 80)
print("12. 净交易金额")
print("=" * 80)

spark.sql(
    """
    SELECT
        ROUND(
            SUM(
                CASE
                    WHEN order_type IN ('SALE', 'RETURN')
                    THEN amount
                    ELSE 0
                END
            ),
            2
        ) AS net_transaction_amount
    FROM online_retail
    """
).show(truncate=False)


# ============================================================
# 14. 完成
# ============================================================

print()
print("=" * 80)
print("Day06 Spark SQL 分析完成")
print("=" * 80)


spark.stop()
