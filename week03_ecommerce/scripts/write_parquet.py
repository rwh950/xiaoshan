from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT = "hdfs://master:9000/user/hadoop/week03/raw/online_retail.csv"
OUTPUT = "hdfs://master:9000/user/hadoop/week03/clean"


spark = (
    SparkSession.builder
    .appName("week04_day05_write_parquet")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark Master:", spark.sparkContext.master)
print("INPUT:", INPUT)
print("OUTPUT:", OUTPUT)


# ============================================================
# 1. 读取原始数据
# ============================================================

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("quote", '"')
    .option("escape", '"')
    .csv(INPUT)
)

raw_count = raw_df.count()

print("raw_count:", raw_count)


# ============================================================
# 2. Schema 标准化
# 与 Day3 保持一致
# ============================================================

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


# ============================================================
# 3. 去重
# ============================================================

dedup_df = standard_df.dropDuplicates()

dedup_count = dedup_df.count()

duplicate_count = raw_count - dedup_count

print("duplicate_count:", duplicate_count)
print("dedup_count:", dedup_count)


# ============================================================
# 4. 五分类
# ============================================================

clean_df = (
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

    .withColumn(
        "amount",
        F.round(
            F.col("quantity") * F.col("unit_price"),
            2
        )
    )

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

    .withColumn(
        "is_customer_missing",
        F.col("customer_id").isNull()
    )

    .withColumn(
        "is_description_missing",
        F.col("description").isNull()
    )

    .withColumn(
        "process_time",
        F.current_timestamp()
    )
)


# ============================================================
# 5. 写 Parquet
# ============================================================

print("开始写 Parquet...")

(
    clean_df
    .write
    .mode("overwrite")
    .partitionBy(
        "invoice_year",
        "invoice_month"
    )
    .parquet(OUTPUT)
)

print("Parquet 写入完成")


# ============================================================
# 6. 最终统计
# ============================================================

print("clean_count:", clean_df.count())

clean_df.groupBy(
    "order_type"
).count().orderBy(
    "order_type"
).show()

spark.stop()
