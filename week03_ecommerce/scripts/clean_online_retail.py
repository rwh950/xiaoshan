import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 参数
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Week04 Day02 - Schema standardization and anomaly profiling"
    )

    parser.add_argument(
        "--input",
        required=True,
        help="HDFS input CSV path",
    )

    return parser.parse_args()


# ============================================================
# 输出标题
# ============================================================

def print_section(title):
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)


# ============================================================
# 主程序
# ============================================================

def main():
    args = parse_args()

    # --------------------------------------------------------
    # 1. 创建 SparkSession
    #
    # 不在这里写 .master("local[*]")
    # Spark Master 由 spark-submit --master 指定
    # --------------------------------------------------------

    spark = (
        SparkSession.builder
        .appName("week04_day02_schema_profile")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print_section("Week04 Day02 - Schema 标准化与异常数据画像")

    print(f"输入路径    : {args.input}")
    print(f"Spark Master: {spark.sparkContext.master}")
    print(f"App Name    : {spark.sparkContext.appName}")

    # --------------------------------------------------------
    # 2. 原始数据全部按 String 读取
    #
    # Day01 使用 inferSchema=true 是为了观察
    # Day02 改为 false，是为了自己控制类型转换
    # --------------------------------------------------------

    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("quote", '"')
        .option("escape", '"')
        .csv(args.input)
    )

    raw_df.cache()

    raw_count = raw_df.count()

    print_section("1. 原始数据基本信息")

    print(f"原始数据总行数: {raw_count}")
    print(f"字段数量      : {len(raw_df.columns)}")

    raw_df.printSchema()

    print()
    print("原始字段：")

    for index, name in enumerate(raw_df.columns, start=1):
        print(f"{index:02d}. {name}")

    # --------------------------------------------------------
    # 3. 字段重命名
    # --------------------------------------------------------

    renamed_df = (
        raw_df
        .withColumnRenamed("invoice_no", "invoice_no_raw")
        .withColumnRenamed("stock_code", "stock_code_raw")
        .withColumnRenamed("description", "description_raw")
        .withColumnRenamed("quantity", "quantity_raw")
        .withColumnRenamed("invoice_date", "invoice_date_raw")
        .withColumnRenamed("unit_price", "unit_price_raw")
        .withColumnRenamed("customer_id", "customer_id_raw")
        .withColumnRenamed("country", "country_raw")
    )

    # --------------------------------------------------------
    # 4. 字符串基础标准化
    #
    # trim：
    # 删除字符串前后的空格
    #
    # when(... == "", None)：
    # 把空字符串转换成真正的 null
    # --------------------------------------------------------

    normalized_df = (
        renamed_df

        .withColumn(
            "invoice_no",
            F.when(
                F.trim(F.col("invoice_no_raw")) == "",
                None
            ).otherwise(
                F.trim(F.col("invoice_no_raw"))
            )
        )

        .withColumn(
            "stock_code",
            F.when(
                F.trim(F.col("stock_code_raw")) == "",
                None
            ).otherwise(
                F.trim(F.col("stock_code_raw"))
            )
        )

        .withColumn(
            "description",
            F.when(
                F.trim(F.col("description_raw")) == "",
                None
            ).otherwise(
                F.trim(F.col("description_raw"))
            )
        )

        .withColumn(
            "country",
            F.when(
                F.trim(F.col("country_raw")) == "",
                None
            ).otherwise(
                F.trim(F.col("country_raw"))
            )
        )
    )

    # --------------------------------------------------------
    # 5. Quantity 类型标准化
    # --------------------------------------------------------

    normalized_df = normalized_df.withColumn(
        "quantity",
        F.trim(F.col("quantity_raw")).cast("int")
    )

    # --------------------------------------------------------
    # 6. UnitPrice 类型标准化
    # --------------------------------------------------------

    normalized_df = normalized_df.withColumn(
        "unit_price",
        F.trim(F.col("unit_price_raw")).cast("double")
    )

    # --------------------------------------------------------
    # 7. InvoiceDate 类型标准化
    #
    # 由于前面 Excel -> CSV 时通常会生成：
    #
    # 2010-12-01 08:26:00
    #
    # 因此这里按 yyyy-MM-dd HH:mm:ss 转换。
    # --------------------------------------------------------

    normalized_df = normalized_df.withColumn(
        "invoice_date",
        F.to_timestamp(
            F.trim(F.col("invoice_date_raw")),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    # --------------------------------------------------------
    # 8. CustomerID 标准化
    #
    # 原始 CustomerID 可能出现：
    #
    # 17850
    # 17850.0
    #
    # 最终统一成字符串 "17850"
    # --------------------------------------------------------

    normalized_df = normalized_df.withColumn(
        "customer_id",
        F.when(
            F.col("customer_id_raw").isNull()
            | (F.trim(F.col("customer_id_raw")) == ""),
            None
        ).otherwise(
            F.regexp_replace(
                F.trim(F.col("customer_id_raw")),
                r"\.0$",
                ""
            )
        )
    )

    # --------------------------------------------------------
    # 9. 最终标准字段
    #
    # 注意：
    # 今天保留 raw 字段是为了方便排错。
    # Day03 正式清洗时再决定是否移除 raw 字段。
    # --------------------------------------------------------

    profile_df = normalized_df.select(
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "customer_id",
        "country",

        "invoice_no_raw",
        "stock_code_raw",
        "description_raw",
        "quantity_raw",
        "invoice_date_raw",
        "unit_price_raw",
        "customer_id_raw",
        "country_raw",
    )

    profile_df.cache()

    # --------------------------------------------------------
    # 10. Schema 检查
    # --------------------------------------------------------

    print_section("2. 标准化后的 Schema")

    profile_df.select(
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "customer_id",
        "country",
    ).printSchema()

    print()
    print("标准字段类型：")

    standard_df = profile_df.select(
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "customer_id",
        "country",
    )

    for column_name, data_type in standard_df.dtypes:
        print(f"{column_name:<20} {data_type}")

    # --------------------------------------------------------
    # 11. 展示转换结果
    # --------------------------------------------------------

    print_section("3. 标准化数据示例")

    standard_df.show(
        20,
        truncate=False
    )

    # ========================================================
    # 数据类型转换失败检查
    # ========================================================

    # --------------------------------------------------------
    # 12. Quantity 转换失败
    #
    # 原始值存在，但转换后变成 null
    # --------------------------------------------------------

    invalid_quantity_df = profile_df.filter(
        F.col("quantity_raw").isNotNull()
        & (F.trim(F.col("quantity_raw")) != "")
        & F.col("quantity").isNull()
    )

    invalid_quantity_count = invalid_quantity_df.count()

    # --------------------------------------------------------
    # 13. UnitPrice 转换失败
    # --------------------------------------------------------

    invalid_price_df = profile_df.filter(
        F.col("unit_price_raw").isNotNull()
        & (F.trim(F.col("unit_price_raw")) != "")
        & F.col("unit_price").isNull()
    )

    invalid_price_count = invalid_price_df.count()

    # --------------------------------------------------------
    # 14. InvoiceDate 转换失败
    # --------------------------------------------------------

    invalid_date_df = profile_df.filter(
        F.col("invoice_date_raw").isNotNull()
        & (F.trim(F.col("invoice_date_raw")) != "")
        & F.col("invoice_date").isNull()
    )

    invalid_date_count = invalid_date_df.count()

    print_section("4. 数据类型转换失败统计")

    print(f"Quantity 转换失败   : {invalid_quantity_count}")
    print(f"UnitPrice 转换失败  : {invalid_price_count}")
    print(f"InvoiceDate 转换失败: {invalid_date_count}")

    if invalid_quantity_count > 0:
        print()
        print("Quantity 转换失败示例：")
        invalid_quantity_df.show(20, truncate=False)

    if invalid_price_count > 0:
        print()
        print("UnitPrice 转换失败示例：")
        invalid_price_df.show(20, truncate=False)

    if invalid_date_count > 0:
        print()
        print("InvoiceDate 转换失败示例：")
        invalid_date_df.show(20, truncate=False)

    # ========================================================
    # 缺失值统计
    # ========================================================

    print_section("5. 关键字段缺失统计")

    missing_invoice_no_count = (
        profile_df
        .filter(F.col("invoice_no").isNull())
        .count()
    )

    missing_stock_code_count = (
        profile_df
        .filter(F.col("stock_code").isNull())
        .count()
    )

    missing_description_count = (
        profile_df
        .filter(F.col("description").isNull())
        .count()
    )

    missing_customer_count = (
        profile_df
        .filter(F.col("customer_id").isNull())
        .count()
    )

    missing_country_count = (
        profile_df
        .filter(F.col("country").isNull())
        .count()
    )

    print(f"invoice_no 缺失 : {missing_invoice_no_count}")
    print(f"stock_code 缺失 : {missing_stock_code_count}")
    print(f"description 缺失: {missing_description_count}")
    print(f"customer_id 缺失: {missing_customer_count}")
    print(f"country 缺失    : {missing_country_count}")

    # ========================================================
    # InvoiceNo C开头与Quantity负数关系
    # ========================================================

    # --------------------------------------------------------
    # 15. 创建两个辅助判断字段
    #
    # is_cancel_invoice
    # is_negative_quantity
    # --------------------------------------------------------

    analysis_df = (
        profile_df

        .withColumn(
            "is_cancel_invoice",
            F.when(
                F.upper(
                    F.trim(F.col("invoice_no"))
                ).startswith("C"),
                True
            ).otherwise(False)
        )

        .withColumn(
            "is_negative_quantity",
            F.when(
                F.col("quantity") < 0,
                True
            ).otherwise(False)
        )
    )

    analysis_df.cache()

    # --------------------------------------------------------
    # A：
    # Quantity < 0
    # 并且 InvoiceNo C开头
    # --------------------------------------------------------

    negative_and_cancel_df = analysis_df.filter(
        (F.col("quantity") < 0)
        & (F.col("is_cancel_invoice") == True)
    )

    negative_and_cancel_count = negative_and_cancel_df.count()

    # --------------------------------------------------------
    # B：
    # Quantity < 0
    # 但 InvoiceNo 非C开头
    # --------------------------------------------------------

    negative_not_cancel_df = analysis_df.filter(
        (F.col("quantity") < 0)
        & (F.col("is_cancel_invoice") == False)
    )

    negative_not_cancel_count = negative_not_cancel_df.count()

    # --------------------------------------------------------
    # C：
    # InvoiceNo C开头
    # 但是 Quantity >= 0
    # --------------------------------------------------------

    cancel_nonnegative_df = analysis_df.filter(
        (F.col("is_cancel_invoice") == True)
        & (F.col("quantity") >= 0)
    )

    cancel_nonnegative_count = cancel_nonnegative_df.count()

    print_section("6. Quantity负数 与 C开头订单关系")

    print(
        "A. Quantity < 0 且 InvoiceNo C开头 : "
        f"{negative_and_cancel_count}"
    )

    print(
        "B. Quantity < 0 但 InvoiceNo 非C开头: "
        f"{negative_not_cancel_count}"
    )

    print(
        "C. InvoiceNo C开头但 Quantity >= 0  : "
        f"{cancel_nonnegative_count}"
    )

    print()
    print("校验：")
    print(
        "负数量总数 A+B = "
        f"{negative_and_cancel_count + negative_not_cancel_count}"
    )

    print_section("7. 非C开头但 Quantity < 0 的记录示例")

    negative_not_cancel_df.select(
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "unit_price",
        "customer_id",
        "country",
    ).show(
        50,
        truncate=False
    )

    print_section("8. C开头但 Quantity >= 0 的记录示例")

    cancel_nonnegative_df.select(
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "unit_price",
        "customer_id",
        "country",
    ).show(
        50,
        truncate=False
    )

    # ========================================================
    # UnitPrice = 0 分析
    # ========================================================

    zero_price_df = analysis_df.filter(
        F.col("unit_price") == 0
    )

    zero_price_count = zero_price_df.count()

    print_section("9. UnitPrice = 0 数据画像")

    print(f"UnitPrice = 0 总数: {zero_price_count}")

    print()
    print("UnitPrice = 0 示例：")

    zero_price_df.select(
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "customer_id",
        "country",
    ).show(
        50,
        truncate=False
    )

    print()
    print("UnitPrice = 0 最常见 Description：")

    (
        zero_price_df
        .groupBy("description")
        .count()
        .orderBy(F.desc("count"))
        .show(
            30,
            truncate=False
        )
    )

    print()
    print("UnitPrice = 0 最常见 StockCode：")

    (
        zero_price_df
        .groupBy("stock_code")
        .count()
        .orderBy(F.desc("count"))
        .show(
            30,
            truncate=False
        )
    )

    # ========================================================
    # UnitPrice < 0
    # ========================================================

    negative_price_df = analysis_df.filter(
        F.col("unit_price") < 0
    )

    negative_price_count = negative_price_df.count()

    print_section("10. UnitPrice < 0 完整记录")

    print(f"UnitPrice < 0 总数: {negative_price_count}")

    negative_price_df.select(
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "customer_id",
        "country",
    ).show(
        100,
        truncate=False
    )

    # ========================================================
    # 完全重复数据
    # ========================================================

    print_section("11. 完全重复记录检查")

    standard_count = standard_df.count()

    distinct_count = standard_df.dropDuplicates().count()

    duplicate_count = standard_count - distinct_count

    print(f"标准化后总行数 : {standard_count}")
    print(f"去重后总行数   : {distinct_count}")
    print(f"完全重复记录数量: {duplicate_count}")

    # ========================================================
    # 国家名称基础检查
    # ========================================================

    print_section("12. Country 分布 TOP 20")

    (
        standard_df
        .groupBy("country")
        .count()
        .orderBy(F.desc("count"))
        .show(
            20,
            truncate=False
        )
    )

    # ========================================================
    # Day02 核心总结
    # ========================================================

    # ========================================================
    # Day02 最终交叉验证
    # ========================================================
    
    print_section("13. Day02 最终交叉验证")
    
    # --------------------------------------------------------
    # 1. 非C开头负数量的价格分布
    # --------------------------------------------------------
    
    negative_not_cancel_zero_price_count = (
        negative_not_cancel_df
        .filter(F.col("unit_price") == 0)
        .count()
    )
    
    negative_not_cancel_positive_price_count = (
        negative_not_cancel_df
        .filter(F.col("unit_price") > 0)
        .count()
    )
    
    negative_not_cancel_negative_price_count = (
        negative_not_cancel_df
        .filter(F.col("unit_price") < 0)
        .count()
    )
    
    print(
        "非C负数量 + UnitPrice = 0 : "
        f"{negative_not_cancel_zero_price_count}"
    )
    
    print(
        "非C负数量 + UnitPrice > 0 : "
        f"{negative_not_cancel_positive_price_count}"
    )
    
    print(
        "非C负数量 + UnitPrice < 0 : "
        f"{negative_not_cancel_negative_price_count}"
    )
    
    
    # --------------------------------------------------------
    # 2. 零价格记录的 Quantity 分布
    # --------------------------------------------------------
    
    zero_price_negative_quantity_count = (
        zero_price_df
        .filter(F.col("quantity") < 0)
        .count()
    )
    
    zero_price_positive_quantity_count = (
        zero_price_df
        .filter(F.col("quantity") > 0)
        .count()
    )
    
    zero_price_cancel_count = (
        zero_price_df
        .filter(F.col("is_cancel_invoice") == True)
        .count()
    )
    
    print()
    print(
        "零价格 + Quantity < 0 : "
        f"{zero_price_negative_quantity_count}"
    )
    
    print(
        "零价格 + Quantity > 0 : "
        f"{zero_price_positive_quantity_count}"
    )
    
    print(
        "零价格 + C开头订单     : "
        f"{zero_price_cancel_count}"
    )
    
    
    # --------------------------------------------------------
    # 3. Description NULL 与零价格关系
    # --------------------------------------------------------
    
    missing_description_zero_price_count = (
        analysis_df
        .filter(
            F.col("description").isNull()
            & (F.col("unit_price") == 0)
        )
        .count()
    )
    
    missing_description_nonzero_price_count = (
        analysis_df
        .filter(
            F.col("description").isNull()
            & (F.col("unit_price") != 0)
        )
        .count()
    )
    
    print()
    print(
        "Description NULL + UnitPrice = 0 : "
        f"{missing_description_zero_price_count}"
    )
    
    print(
        "Description NULL + UnitPrice != 0: "
        f"{missing_description_nonzero_price_count}"
    )
    
    print_section("14. Day02 核心统计汇总")

    print(f"raw_count                 : {raw_count}")
    print(f"invalid_quantity_count    : {invalid_quantity_count}")
    print(f"invalid_price_count       : {invalid_price_count}")
    print(f"invalid_date_count        : {invalid_date_count}")
    print(f"missing_invoice_no_count  : {missing_invoice_no_count}")
    print(f"missing_stock_code_count  : {missing_stock_code_count}")
    print(f"missing_description_count : {missing_description_count}")
    print(f"missing_customer_count    : {missing_customer_count}")
    print(f"missing_country_count     : {missing_country_count}")
    print(f"negative_and_cancel       : {negative_and_cancel_count}")
    print(f"negative_not_cancel       : {negative_not_cancel_count}")
    print(f"cancel_nonnegative        : {cancel_nonnegative_count}")
    print(f"zero_price_count          : {zero_price_count}")
    print(f"negative_price_count      : {negative_price_count}")
    print(f"duplicate_count           : {duplicate_count}")

    print_section("Day02 Schema标准化与异常画像完成")

    # --------------------------------------------------------
    # 清理缓存并关闭 Spark
    # --------------------------------------------------------

    analysis_df.unpersist()
    profile_df.unpersist()
    raw_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
