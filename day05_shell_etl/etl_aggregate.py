#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Day 5 PySpark ETL

从Hadoop完全分布式HDFS读取学生成绩，
完成清洗和聚合，然后将结果写回HDFS。
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    count,
    max as spark_max,
    round as spark_round,
    sum as spark_sum,
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def main() -> None:
    if len(sys.argv) != 3:
        print(
            "用法：etl_aggregate.py <input_hdfs_uri> <output_hdfs_uri>",
            file=sys.stderr,
        )
        sys.exit(2)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # 不要在代码中写 .master("local[*]")
    # Master地址由spark-submit的--master参数决定。
    spark = (
        SparkSession.builder
        .appName("Day5_ETL_Aggregate")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("学号", StringType(), nullable=False),
            StructField("姓名", StringType(), nullable=False),
            StructField("科目", StringType(), nullable=False),
            StructField("分数", IntegerType(), nullable=True),
            StructField("班级", StringType(), nullable=False),
        ]
    )

    try:
        print("=" * 60)
        print(f"[ETL] Spark Master：{spark.sparkContext.master}")
        print(f"[ETL] 输入路径：{input_path}")
        print(f"[ETL] 输出路径：{output_path}")
        print("=" * 60)

        source_df = (
            spark.read
            .option("header", True)
            .schema(schema)
            .csv(input_path)
        )

        source_count = source_df.count()
        print(f"[ETL] 原始数据行数：{source_count}")

        source_df.show(20, truncate=False)

        clean_df = (
            source_df
            .filter(source_df["分数"].isNotNull())
            .filter(
                (source_df["分数"] >= 0)
                & (source_df["分数"] <= 100)
            )
            .dropDuplicates(["学号", "科目"])
        )

        clean_count = clean_df.count()
        print(f"[ETL] 清洗后数据行数：{clean_count}")

        result_df = (
            clean_df
            .groupBy("学号", "姓名", "班级")
            .agg(
                spark_round(avg("分数"), 1).alias("平均分"),
                spark_sum("分数").alias("总分"),
                spark_max("分数").alias("最高分"),
                count("*").alias("科目数"),
            )
            .orderBy("总分", ascending=False)
        )

        print("[ETL] 聚合结果：")
        result_df.show(truncate=False)

        # 数据量很小，合并成一个part文件，方便Shell验证。
        (
            result_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(output_path)
        )

        print("[ETL] 数据写入完成")
        print("[ETL] 任务执行成功")

    finally:
        spark.stop()
        print("[ETL] SparkSession已关闭")


if __name__ == "__main__":
    main()
