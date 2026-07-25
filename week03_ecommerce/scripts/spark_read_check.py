#!/usr/bin/env python3

from __future__ import annotations

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


EXPECTED_COLUMNS = [
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "unit_price",
    "customer_id",
    "country",
]


def main() -> None:
    """使用Spark Standalone读取HDFS中的原始CSV。"""

    if len(sys.argv) != 2:
        print(
            "用法：spark-submit spark_read_check.py <HDFS输入路径>",
            file=sys.stderr,
        )
        print(
            "示例：spark-submit "
            "--master spark://main:7077 "
            "spark_read_check.py "
            "hdfs://master:9000/user/hadoop/ecommerce/raw/online_retail.csv",
            file=sys.stderr,
        )
        sys.exit(1)

    input_path = sys.argv[1].strip()

    if not input_path:
        print("错误：HDFS输入路径不能为空。", file=sys.stderr)
        sys.exit(1)

    spark = None
    dataframe = None

    try:
        print("=" * 72)
        print("第三周 Day 5：Spark读取HDFS验证")
        print("=" * 72)
        print(f"输入路径：{input_path}")

        # 不在代码中指定 .master("local[*]")。
        # Spark Master由spark-submit的--master参数指定。
        spark = (
            SparkSession.builder
            .appName("Week03RawDataCheck")
            .getOrCreate()
        )

        spark_context = spark.sparkContext
        spark_context.setLogLevel("WARN")

        print("\n[1/6] Spark运行信息")
        print(f"Spark版本：{spark.version}")
        print(f"Application ID：{spark_context.applicationId}")
        print(f"实际Master：{spark_context.master}")
        print(f"默认并行度：{spark_context.defaultParallelism}")
        print(f"Driver主机：{spark_context.getConf().get('spark.driver.host')}")

        print("\n[2/6] 读取HDFS CSV")

        dataframe = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("mode", "FAILFAST")
            .csv(input_path)
        )

        print("CSV读取计划创建成功")
        print(f"输入文件数量：{len(dataframe.inputFiles())}")

        print("\n[3/6] 检查字段")

        actual_columns = dataframe.columns

        print("实际字段：")

        for index, column in enumerate(actual_columns, start=1):
            print(f"  {index}. {column}")

        missing_columns = [
            column
            for column in EXPECTED_COLUMNS
            if column not in actual_columns
        ]

        extra_columns = [
            column
            for column in actual_columns
            if column not in EXPECTED_COLUMNS
        ]

        if missing_columns:
            print(
                f"错误：缺少字段：{missing_columns}",
                file=sys.stderr,
            )
            sys.exit(2)

        if extra_columns:
            print(f"警告：发现额外字段：{extra_columns}")

        if actual_columns == EXPECTED_COLUMNS:
            print("字段名称和顺序检查通过")
        else:
            print(
                "警告：字段全部存在，但字段顺序与预期不同。"
            )

        # 缓存后，由count触发第一次完整计算。
        dataframe.cache()

        print("\n[4/6] 触发Spark计算")

        row_count = dataframe.count()
        partition_count = dataframe.rdd.getNumPartitions()

        print(f"记录数量：{row_count:,}")
        print(f"分区数量：{partition_count}")

        print("\n[5/6] 查看Schema和样例")

        dataframe.printSchema()

        dataframe.show(
            10,
            truncate=60,
            vertical=False,
        )

        print("\n[6/6] 统计字段空值")

        null_expressions = [
            F.sum(
                F.when(
                    F.col(column).isNull(),
                    1,
                ).otherwise(0)
            ).cast("long").alias(column)
            for column in dataframe.columns
        ]

        dataframe.select(
            null_expressions
        ).show(
            truncate=False
        )

        print("\n" + "=" * 72)
        print("Spark读取验证成功")
        print(f"输入路径：{input_path}")
        print(f"记录数量：{row_count:,}")
        print(f"分区数量：{partition_count}")
        print(f"Application ID：{spark_context.applicationId}")
        print("=" * 72)

    except Exception as error:
        print(
            "\nSpark读取验证失败："
            f"{type(error).__name__}: {error}",
            file=sys.stderr,
        )
        sys.exit(1)

    finally:
        if dataframe is not None:
            try:
                dataframe.unpersist(blocking=False)
            except Exception:
                pass

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
