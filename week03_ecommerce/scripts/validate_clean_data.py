import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(
        description="Week04 Day04 quality check"
    )

    parser.add_argument(
        "--input",
        required=True,
        help="HDFS raw csv path",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # ========================================================
    # 1. Spark 初始化
    # ========================================================

    spark = (
        SparkSession.builder
        .appName("week04_day04_quality_check")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Spark Master:", spark.sparkContext.master)
    print("App Name:", spark.sparkContext.appName)

    # ========================================================
    # 2. 读取 HDFS 原始数据
    # ========================================================

    input_path = args.input

    print("Input Path:", input_path)

    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("quote", '"')
        .option("escape", '"')
        .csv(input_path)
    )

    raw_count = raw_df.count()

    print("字段数量:", len(raw_df.columns))
    print("字段:", raw_df.columns)
    print("raw_count:", raw_count)

    # ========================================================
    # 3. 字段标准化
    # 与 Day03 保持一致
    # ========================================================

    standard_df = raw_df.select(

        F.when(
            F.trim(F.col("invoice_no")) == "",
            None
        ).otherwise(
            F.trim(F.col("invoice_no"))
        ).alias("invoice_no"),

        F.when(
            F.trim(F.col("stock_code")) == "",
            None
        ).otherwise(
            F.trim(F.col("stock_code"))
        ).alias("stock_code"),

        F.when(
            F.trim(F.col("description")) == "",
            None
        ).otherwise(
            F.trim(F.col("description"))
        ).alias("description"),

        F.trim(
            F.col("quantity")
        ).cast("int").alias("quantity"),

        F.to_timestamp(
            F.trim(F.col("invoice_date")),
            "yyyy-MM-dd HH:mm:ss"
        ).alias("invoice_date"),

        F.trim(
            F.col("unit_price")
        ).cast("double").alias("unit_price"),

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

        F.when(
            F.trim(F.col("country")) == "",
            None
        ).otherwise(
            F.trim(F.col("country"))
        ).alias("country"),
    )

    standard_count = standard_df.count()

    print("标准化字段:")
    standard_df.printSchema()
    print("standard_count:", standard_count)

    # ========================================================
    # 4. 去重
    # ========================================================

    dedup_df = standard_df.dropDuplicates()

    dedup_count = dedup_df.count()

    duplicate_count = (
        standard_count
        - dedup_count
    )

    check_duplicate = (
        standard_count
        == duplicate_count + dedup_count
    )

    print("duplicate_count:", duplicate_count)
    print("dedup_count:", dedup_count)
    print("原始数据对账:", check_duplicate)

    # ========================================================
    # 5. order_type 分类
    # ========================================================

    classified_df = (
        dedup_df
        .withColumn(
            "order_type",

            F.when(
                F.col("invoice_no").startswith("C"),
                "RETURN"
            )

            .when(
                (F.col("quantity") < 0)
                |
                (F.col("unit_price") < 0),
                "ADJUSTMENT"
            )

            .when(
                F.col("unit_price") == 0,
                "ZERO_PRICE"
            )

            .otherwise(
                "SALE"
            )
        )
    )

    # ========================================================
    # 6. 分类统计
    # ========================================================

    print("order_type统计:")

    (
        classified_df
        .groupBy("order_type")
        .count()
        .orderBy("order_type")
        .show()
    )

    # ========================================================
    # 7. 分类规则检查
    # ========================================================

    bad_sale = (
        classified_df
        .filter(
            (F.col("order_type") == "SALE")
            &
            (
                (F.col("quantity") <= 0)
                |
                (F.col("unit_price") <= 0)
                |
                F.col("invoice_no").startswith("C")
            )
        )
        .count()
    )

    bad_return = (
        classified_df
        .filter(
            (F.col("order_type") == "RETURN")
            &
            (
                ~F.col("invoice_no").startswith("C")
                |
                (F.col("quantity") >= 0)
            )
        )
        .count()
    )

    bad_adjustment = (
        classified_df
        .filter(
            (F.col("order_type") == "ADJUSTMENT")
            &
            (
                (F.col("quantity") >= 0)
                &
                (F.col("unit_price") >= 0)
            )
        )
        .count()
    )

    bad_zero_price = (
        classified_df
        .filter(
            (F.col("order_type") == "ZERO_PRICE")
            &
            (
                (F.col("unit_price") != 0)
                |
                (F.col("quantity") <= 0)
            )
        )
        .count()
    )

    print("bad_sale:", bad_sale)
    print("bad_return:", bad_return)
    print("bad_adjustment:", bad_adjustment)
    print("bad_zero_price:", bad_zero_price)

    # ========================================================
    # 8. amount
    # ========================================================

    amount_df = (
        classified_df
        .withColumn(
            "amount",
            F.round(
                F.col("quantity")
                * F.col("unit_price"),
                2
            )
        )
    )

    # ========================================================
    # 9. amount 检查
    # ========================================================

    bad_amount_null = (
        amount_df
        .filter(
            F.col("amount").isNull()
        )
        .count()
    )

    bad_sale_amount = (
        amount_df
        .filter(
            (F.col("order_type") == "SALE")
            &
            (F.col("amount") <= 0)
        )
        .count()
    )

    bad_return_amount = (
        amount_df
        .filter(
            (F.col("order_type") == "RETURN")
            &
            (F.col("amount") >= 0)
        )
        .count()
    )

    print("bad_amount_null:", bad_amount_null)
    print("bad_sale_amount:", bad_sale_amount)
    print("bad_return_amount:", bad_return_amount)

    # ========================================================
    # 10. 日期字段
    # ========================================================

    date_df = (
        amount_df
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
    # 11. 日期检查
    # ========================================================

    bad_date = (
        date_df
        .filter(
            F.col("invoice_date").isNotNull()
            &
            (
                (F.col("invoice_year") != F.year("invoice_date"))
                |
                (F.col("invoice_month") != F.month("invoice_date"))
                |
                (F.col("invoice_day") != F.dayofmonth("invoice_date"))
                |
                (F.col("invoice_hour") != F.hour("invoice_date"))
            )
        )
        .count()
    )

    print("bad_date:", bad_date)

    # ========================================================
    # 12. CustomerID / Description 缺失标记
    # ========================================================

    checked_df = (
        date_df
        .withColumn(
            "is_customer_missing",
            F.col("customer_id").isNull()
        )
        .withColumn(
            "is_description_missing",
            F.col("description").isNull()
        )
    )

    bad_customer_flag = (
        checked_df
        .filter(
            F.col("is_customer_missing")
            !=
            F.col("customer_id").isNull()
        )
        .count()
    )

    bad_description_flag = (
        checked_df
        .filter(
            F.col("is_description_missing")
            !=
            F.col("description").isNull()
        )
        .count()
    )

    print(
        "bad_customer_flag:",
        bad_customer_flag
    )

    print(
        "bad_description_flag:",
        bad_description_flag
    )

    # ========================================================
    # 13. 清洗后重复检查
    # ========================================================

    business_columns = [
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "customer_id",
        "country",
    ]

    business_distinct_count = (
        checked_df
        .select(*business_columns)
        .dropDuplicates()
        .count()
    )

    duplicate_after_clean = (
        checked_df.count()
        - business_distinct_count
    )

    print(
        "duplicate_after_clean:",
        duplicate_after_clean
    )

    # ========================================================
    # 14. 最终状态
    # ========================================================

    all_pass = (
        raw_count == 541909
        and standard_count == 541909
        and duplicate_count == 5268
        and dedup_count == 536641
        and check_duplicate
        and bad_sale == 0
        and bad_return == 0
        and bad_adjustment == 0
        and bad_zero_price == 0
        and bad_amount_null == 0
        and bad_sale_amount == 0
        and bad_return_amount == 0
        and bad_date == 0
        and bad_customer_flag == 0
        and bad_description_flag == 0
        and duplicate_after_clean == 0
    )

    print()
    print("=" * 60)

    if all_pass:
        print("QUALITY_STATUS: PASS")
    else:
        print("QUALITY_STATUS: FAIL")

    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
