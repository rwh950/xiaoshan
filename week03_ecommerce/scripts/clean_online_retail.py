import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 参数
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Week04 Day03 - Online Retail cleaning and classification"
    )

    parser.add_argument(
        "--input",
        required=True,
        help="HDFS raw CSV input path",
    )

    return parser.parse_args()


# ============================================================
# 打印标题
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
    # 1. SparkSession
    #
    # 注意：
    # 不在 Python 中写 .master("local[*]")
    # Spark Master 由 spark-submit --master 指定。
    # --------------------------------------------------------

    spark = (
        SparkSession.builder
        .appName("week04_day03_clean_classify")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print_section("Week04 Day03 - 正式清洗与业务分类")

    print(f"输入路径    : {args.input}")
    print(f"Spark Master: {spark.sparkContext.master}")
    print(f"App Name    : {spark.sparkContext.appName}")

    # ========================================================
    # 2. 原始 CSV 按字符串读取
    # ========================================================

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

    print_section("1. 原始数据")

    print(f"raw_count: {raw_count}")

    raw_df.printSchema()

    # ========================================================
    # 3. 字段标准化
    # ========================================================

    standard_df = raw_df.select(

        # invoice_no
        F.when(
            F.trim(F.col("invoice_no")) == "",
            None
        ).otherwise(
            F.trim(F.col("invoice_no"))
        ).alias("invoice_no"),

        # stock_code
        F.when(
            F.trim(F.col("stock_code")) == "",
            None
        ).otherwise(
            F.trim(F.col("stock_code"))
        ).alias("stock_code"),

        # description
        F.when(
            F.trim(F.col("description")) == "",
            None
        ).otherwise(
            F.trim(F.col("description"))
        ).alias("description"),

        # quantity
        F.trim(
            F.col("quantity")
        ).cast("int").alias("quantity"),

        # invoice_date
        F.to_timestamp(
            F.trim(F.col("invoice_date")),
            "yyyy-MM-dd HH:mm:ss"
        ).alias("invoice_date"),

        # unit_price
        F.trim(
            F.col("unit_price")
        ).cast("double").alias("unit_price"),

        # customer_id
        F.when(
            F.col("customer_id").isNull()
            | (F.trim(F.col("customer_id")) == ""),
            None
        ).otherwise(
            F.regexp_replace(
                F.trim(F.col("customer_id")),
                r"\.0$",
                ""
            )
        ).alias("customer_id"),

        # country
        F.when(
            F.trim(F.col("country")) == "",
            None
        ).otherwise(
            F.trim(F.col("country"))
        ).alias("country"),
    )

    standard_df.cache()

    standard_count = standard_df.count()

    print_section("2. 标准化后 Schema")

    standard_df.printSchema()

    print(f"standard_count: {standard_count}")

    print()
    print("标准化数据示例：")

    standard_df.show(
        20,
        truncate=False
    )

    # ========================================================
    # 4. 去除完全重复记录
    # ========================================================

    dedup_df = standard_df.dropDuplicates()

    dedup_df.cache()

    dedup_count = dedup_df.count()

    duplicate_count = standard_count - dedup_count

    print_section("3. 完全重复记录处理")

    print(f"standard_count : {standard_count}")
    print(f"duplicate_count: {duplicate_count}")
    print(f"dedup_count     : {dedup_count}")

    print()
    print(
        "对账 standard_count = duplicate_count + dedup_count : "
        f"{standard_count == duplicate_count + dedup_count}"
    )

    # ========================================================
    # 5. 增加缺失标记
    #
    # customer_id 和 description 缺失：
    # 不直接删除。
    # ========================================================

    enriched_df = (
        dedup_df

        .withColumn(
            "is_customer_missing",
            F.col("customer_id").isNull()
        )

        .withColumn(
            "is_description_missing",
            F.col("description").isNull()
        )
    )

    # ========================================================
    # 6. REJECTED 判断
    #
    # 当前数据预计这些数量都是0，
    # 但是正式 ETL 仍然保留防御性规则。
    # ========================================================

    rejected_condition = (
        F.col("invoice_no").isNull()
        | F.col("stock_code").isNull()
        | F.col("quantity").isNull()
        | F.col("invoice_date").isNull()
        | F.col("unit_price").isNull()
        | (F.col("quantity") == 0)
    )

    # ========================================================
    # 7. RETURN 判断
    #
    # Day02 已验证：
    #
    # C开头总数                = 9288
    # C开头且 quantity < 0     = 9288
    # C开头且 quantity >= 0    = 0
    # ========================================================

    return_condition = (
        F.upper(
            F.trim(F.col("invoice_no"))
        ).startswith("C")
    )

    # ========================================================
    # 8. ADJUSTMENT 判断
    #
    # 包含：
    #
    # 非C负数量调整
    # Adjust bad debt
    #
    # 注意：
    # RETURN 的判断优先级更高，
    # 所以 C 开头负数量不会落入这里。
    # ========================================================

    adjustment_condition = (
        (F.col("quantity") < 0)
        | (F.col("unit_price") < 0)
    )

    # ========================================================
    # 9. ZERO_PRICE 判断
    #
    # 到达这里时：
    #
    # 已经不是 RETURN
    # 已经不是负数量 ADJUSTMENT
    #
    # 因此 unit_price=0 主要对应
    # Day02 已确认的正数量零价格记录。
    # ========================================================

    zero_price_condition = (
        F.col("unit_price") == 0
    )

    # ========================================================
    # 10. order_type
    #
    # 判断顺序绝对不要乱。
    # ========================================================

    classified_df = enriched_df.withColumn(
        "order_type",

        F.when(
            rejected_condition,
            "REJECTED"
        )

        .when(
            return_condition,
            "RETURN"
        )

        .when(
            adjustment_condition,
            "ADJUSTMENT"
        )

        .when(
            zero_price_condition,
            "ZERO_PRICE"
        )

        .otherwise(
            "SALE"
        )
    )

    # ========================================================
    # 11. reject_reason
    #
    # 当前数据理论上 REJECTED 很可能为0。
    # 仍保留字段供以后生产 ETL 使用。
    # ========================================================

    classified_df = classified_df.withColumn(
        "reject_reason",

        F.when(
            F.col("invoice_no").isNull(),
            "MISSING_INVOICE_NO"
        )

        .when(
            F.col("stock_code").isNull(),
            "MISSING_STOCK_CODE"
        )

        .when(
            F.col("quantity").isNull(),
            "INVALID_QUANTITY"
        )

        .when(
            F.col("invoice_date").isNull(),
            "INVALID_INVOICE_DATE"
        )

        .when(
            F.col("unit_price").isNull(),
            "INVALID_UNIT_PRICE"
        )

        .when(
            F.col("quantity") == 0,
            "ZERO_QUANTITY"
        )

        .otherwise(
            None
        )
    )

    # ========================================================
    # 12. amount
    #
    # 不使用 abs()
    #
    # SALE：
    #   正金额
    #
    # RETURN：
    #   quantity 是负数，因此 amount 是负数
    #
    # ADJUSTMENT：
    #   可以是0或负数
    #
    # ZERO_PRICE：
    #   amount = 0
    # ========================================================

    classified_df = classified_df.withColumn(
        "amount",
        F.round(
            F.col("quantity") * F.col("unit_price"),
            2
        )
    )

    # ========================================================
    # 13. 日期衍生字段
    # ========================================================

    classified_df = (
        classified_df

        .withColumn(
            "invoice_year",
            F.year("invoice_date")
        )

        .withColumn(
            "invoice_month",
            F.month("invoice_date")
        )

        .withColumn(
            "invoice_day",
            F.dayofmonth("invoice_date")
        )

        .withColumn(
            "invoice_hour",
            F.hour("invoice_date")
        )
    )

    # ========================================================
    # 14. ETL处理时间
    # ========================================================

    classified_df = classified_df.withColumn(
        "process_time",
        F.current_timestamp()
    )

    # ========================================================
    # 15. 最终 Clean DataFrame 字段顺序
    # ========================================================

    clean_df = classified_df.select(
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "amount",
        "customer_id",
        "country",

        "order_type",
        "reject_reason",

        "is_customer_missing",
        "is_description_missing",

        "invoice_year",
        "invoice_month",
        "invoice_day",
        "invoice_hour",

        "process_time",
    )

    clean_df.cache()

    clean_count = clean_df.count()

    print_section("4. Clean DataFrame Schema")

    clean_df.printSchema()

    print()
    print(f"clean_count: {clean_count}")

    print()
    print("Clean DataFrame 示例：")

    clean_df.show(
        20,
        truncate=False
    )

    # ========================================================
    # 16. 五类业务数量
    # ========================================================

    print_section("5. order_type 分类统计")

    order_type_count_df = (
        clean_df
        .groupBy("order_type")
        .count()
        .orderBy("order_type")
    )

    order_type_count_df.show(
        truncate=False
    )

    # 单独获取数量，方便后续对账

    sale_count = (
        clean_df
        .filter(F.col("order_type") == "SALE")
        .count()
    )

    return_count = (
        clean_df
        .filter(F.col("order_type") == "RETURN")
        .count()
    )

    adjustment_count = (
        clean_df
        .filter(F.col("order_type") == "ADJUSTMENT")
        .count()
    )

    zero_price_count = (
        clean_df
        .filter(F.col("order_type") == "ZERO_PRICE")
        .count()
    )

    rejected_count = (
        clean_df
        .filter(F.col("order_type") == "REJECTED")
        .count()
    )

    classified_total = (
        sale_count
        + return_count
        + adjustment_count
        + zero_price_count
        + rejected_count
    )

    print()
    print(f"SALE       : {sale_count}")
    print(f"RETURN     : {return_count}")
    print(f"ADJUSTMENT : {adjustment_count}")
    print(f"ZERO_PRICE : {zero_price_count}")
    print(f"REJECTED   : {rejected_count}")

    print()
    print(f"classified_total: {classified_total}")
    print(f"dedup_count      : {dedup_count}")

    print(
        "分类数量对账成功: "
        f"{classified_total == dedup_count}"
    )

    # ========================================================
    # 17. 各类型 amount 汇总
    # ========================================================

    print_section("6. 各 order_type 金额汇总")

    (
        clean_df
        .groupBy("order_type")
        .agg(
            F.count("*").alias("row_count"),
            F.round(
                F.sum("amount"),
                2
            ).alias("amount_sum"),
            F.round(
                F.avg("amount"),
                2
            ).alias("amount_avg"),
        )
        .orderBy("order_type")
        .show(
            truncate=False
        )
    )

    # ========================================================
    # 18. 每类展示10条
    # ========================================================

    for order_type in [
        "SALE",
        "RETURN",
        "ADJUSTMENT",
        "ZERO_PRICE",
        "REJECTED",
    ]:

        print_section(
            f"7. {order_type} 示例"
        )

        (
            clean_df
            .filter(
                F.col("order_type") == order_type
            )
            .show(
                10,
                truncate=False
            )
        )

    # ========================================================
    # 19. SALE规则验证
    #
    # SALE必须：
    #
    # quantity > 0
    # unit_price > 0
    # invoice_no 非C
    # ========================================================

    print_section("8. SALE 数据质量验证")

    invalid_sale_count = (
        clean_df
        .filter(
            (F.col("order_type") == "SALE")
            & (
                (F.col("quantity") <= 0)
                | (F.col("unit_price") <= 0)
                | F.upper(
                    F.col("invoice_no")
                ).startswith("C")
            )
        )
        .count()
    )

    print(
        "不符合 SALE 规则的记录数量: "
        f"{invalid_sale_count}"
    )

    # ========================================================
    # 20. RETURN规则验证
    # ========================================================

    print_section("9. RETURN 数据质量验证")

    invalid_return_count = (
        clean_df
        .filter(
            (F.col("order_type") == "RETURN")
            & ~F.upper(
                F.col("invoice_no")
            ).startswith("C")
        )
        .count()
    )

    print(
        "RETURN 中非C开头记录数量: "
        f"{invalid_return_count}"
    )

    # ========================================================
    # 21. ZERO_PRICE规则验证
    # ========================================================

    print_section("10. ZERO_PRICE 数据质量验证")

    invalid_zero_price_count = (
        clean_df
        .filter(
            (F.col("order_type") == "ZERO_PRICE")
            & (
                (F.col("unit_price") != 0)
                | (F.col("quantity") <= 0)
            )
        )
        .count()
    )

    print(
        "不符合 ZERO_PRICE 规则数量: "
        f"{invalid_zero_price_count}"
    )

    # ========================================================
    # 22. 去重结果验证
    # ========================================================

    print_section("11. 去重结果验证")

    clean_distinct_count = (
        clean_df
        .select(
            "invoice_no",
            "stock_code",
            "description",
            "quantity",
            "invoice_date",
            "unit_price",
            "customer_id",
            "country",
        )
        .dropDuplicates()
        .count()
    )

    duplicate_after_clean = (
        clean_count
        - clean_distinct_count
    )

    print(
        "clean_df 中完全重复业务记录: "
        f"{duplicate_after_clean}"
    )

    # ========================================================
    # 23. 缺失字段统计
    # ========================================================

    print_section("12. 缺失字段统计")

    missing_customer_count = (
        clean_df
        .filter(
            F.col("is_customer_missing") == True
        )
        .count()
    )

    missing_description_count = (
        clean_df
        .filter(
            F.col("is_description_missing") == True
        )
        .count()
    )

    print(
        "customer_id 缺失: "
        f"{missing_customer_count}"
    )

    print(
        "description 缺失: "
        f"{missing_description_count}"
    )

    # ========================================================
    # 24. 最终验收汇总
    # ========================================================

    print_section("13. Day03 最终验收汇总")

    print(f"raw_count                : {raw_count}")
    print(f"duplicate_count          : {duplicate_count}")
    print(f"dedup_count              : {dedup_count}")

    print(f"sale_count               : {sale_count}")
    print(f"return_count             : {return_count}")
    print(f"adjustment_count         : {adjustment_count}")
    print(f"zero_price_count         : {zero_price_count}")
    print(f"rejected_count           : {rejected_count}")

    print(f"classified_total         : {classified_total}")

    print(f"invalid_sale_count       : {invalid_sale_count}")
    print(f"invalid_return_count     : {invalid_return_count}")
    print(f"invalid_zero_price_count : {invalid_zero_price_count}")

    print(f"duplicate_after_clean    : {duplicate_after_clean}")

    print(f"missing_customer_count   : {missing_customer_count}")
    print(f"missing_description_count: {missing_description_count}")

    print()
    print(
        "原始数据对账: "
        f"{raw_count == duplicate_count + dedup_count}"
    )

    print(
        "分类数据对账: "
        f"{dedup_count == classified_total}"
    )

    print_section("Day03 正式清洗与业务分类完成")

    # ========================================================
    # 25. 清理缓存
    # ========================================================

    clean_df.unpersist()
    dedup_df.unpersist()
    standard_df.unpersist()
    raw_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
