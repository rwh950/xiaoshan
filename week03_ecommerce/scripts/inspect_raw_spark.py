import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(
        description="Week04 Day01 - Inspect Online Retail raw data"
    )

    parser.add_argument(
        "--input",
        required=True,
        help="HDFS input CSV path",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # ---------------------------------------------------------
    # 1. 创建 SparkSession
    #
    # 注意：
    # 这里不要写 .master("local[*]")
    # master 地址由 spark-submit --master 指定
    # ---------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("week04_day01_inspect_raw")
        .getOrCreate()
    )

    # 减少控制台无关日志
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("Week04 Day01 - Online Retail 原始数据检查")
    print("=" * 80)

    print(f"输入路径: {args.input}")
    print(f"Spark Master: {spark.sparkContext.master}")
    print(f"Spark App Name: {spark.sparkContext.appName}")

    # ---------------------------------------------------------
    # 2. 从 HDFS 读取 CSV
    # ---------------------------------------------------------
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(args.input)
    )

    # 后面会重复执行多个统计操作。
    # 缓存后，避免每次都重新读取 CSV。
    df.cache()

    # ---------------------------------------------------------
    # 3. 数据总行数
    # ---------------------------------------------------------
    row_count = df.count()

    print()
    print("=" * 80)
    print("1. 原始数据总行数")
    print("=" * 80)
    print(f"总行数: {row_count}")

    # ---------------------------------------------------------
    # 4. 字段名称
    # ---------------------------------------------------------
    print()
    print("=" * 80)
    print("2. 字段名称")
    print("=" * 80)

    for index, column_name in enumerate(df.columns, start=1):
        print(f"{index:02d}. {column_name}")

    print(f"字段数量: {len(df.columns)}")

    # ---------------------------------------------------------
    # 5. Schema
    # ---------------------------------------------------------
    print()
    print("=" * 80)
    print("3. Spark Schema")
    print("=" * 80)

    df.printSchema()

    # 同时打印字段和 Spark 类型
    print()
    print("字段类型明细:")

    for column_name, data_type in df.dtypes:
        print(f"{column_name:<20} {data_type}")

    # ---------------------------------------------------------
    # 6. 前 20 行
    # ---------------------------------------------------------
    print()
    print("=" * 80)
    print("4. 前 20 行")
    print("=" * 80)

    df.show(
        n=20,
        truncate=False,
        vertical=False,
    )

    # ---------------------------------------------------------
    # 7. 每一个字段的空值数量
    #
    # 同时处理：
    #   null
    #   ""
    #   "   "
    # ---------------------------------------------------------
    print()
    print("=" * 80)
    print("5. 每个字段的空值数量")
    print("=" * 80)

    null_count_exprs = []

    for column_name in df.columns:
        expression = F.sum(
            F.when(
                F.col(column_name).isNull()
                | (
                    F.trim(
                        F.col(column_name).cast("string")
                    ) == ""
                ),
                1,
            ).otherwise(0)
        ).alias(column_name)

        null_count_exprs.append(expression)

    null_counts = df.select(*null_count_exprs)

    null_counts.show(
        truncate=False,
        vertical=True,
    )

    # ---------------------------------------------------------
    # 8. Quantity 范围
    # ---------------------------------------------------------
    print()
    print("=" * 80)
    print("6. Quantity 最小值 / 最大值")
    print("=" * 80)

    df.select(
        F.min("quantity").alias("quantity_min"),
        F.max("quantity").alias("quantity_max"),
    ).show(truncate=False)

    # ---------------------------------------------------------
    # 9. UnitPrice 范围
    # ---------------------------------------------------------
    print()
    print("=" * 80)
    print("7. UnitPrice 最小值 / 最大值")
    print("=" * 80)

    df.select(
        F.min("unit_price").alias("unit_price_min"),
        F.max("unit_price").alias("unit_price_max"),
    ).show(truncate=False)

    # ---------------------------------------------------------
    # 10. Country 去重数量
    # ---------------------------------------------------------
    print()
    print("=" * 80)
    print("8. Country 去重数量")
    print("=" * 80)

    valid_country_df = (
        df
        .filter(
            F.col("country").isNotNull()
            & (F.trim(F.col("country")) != "")
        )
        .select("country")
        .distinct()
    )

    country_count = valid_country_df.count()

    print(f"Country 去重数量: {country_count}")

    print()
    print("Country 示例:")

    valid_country_df.orderBy("country").show(
        100,
        truncate=False,
    )

    # ---------------------------------------------------------
    # 11. Quantity / UnitPrice summary
    # ---------------------------------------------------------
    print()
    print("=" * 80)
    print("9. Quantity / UnitPrice 统计摘要")
    print("=" * 80)

    df.select(
        "quantity",
        "unit_price",
    ).summary().show(truncate=False)

    # ---------------------------------------------------------
    # 12. 部分关键异常数量
    #
    # Day01 暂时只观察，不删除数据。
    # ---------------------------------------------------------
    print()
    print("=" * 80)
    print("10. 初步异常统计")
    print("=" * 80)

    quantity_zero_count = (
        df
        .filter(F.col("quantity") == 0)
        .count()
    )

    quantity_negative_count = (
        df
        .filter(F.col("quantity") < 0)
        .count()
    )

    price_zero_count = (
        df
        .filter(F.col("unit_price") == 0)
        .count()
    )

    price_negative_count = (
        df
        .filter(F.col("unit_price") < 0)
        .count()
    )

    cancel_invoice_count = (
        df
        .filter(
            F.upper(
                F.col("invoice_no").cast("string")
            ).startswith("C")
        )
        .count()
    )

    print(f"Quantity = 0       : {quantity_zero_count}")
    print(f"Quantity < 0       : {quantity_negative_count}")
    print(f"UnitPrice = 0      : {price_zero_count}")
    print(f"UnitPrice < 0      : {price_negative_count}")
    print(f"InvoiceNo 以 C 开头: {cancel_invoice_count}")

    # ---------------------------------------------------------
    # 13. 完成
    # ---------------------------------------------------------
    print()
    print("=" * 80)
    print("Day01 原始数据检查完成")
    print("=" * 80)

    # 释放缓存
    df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
