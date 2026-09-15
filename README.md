# xiaoshan

个人学习与练习项目集合（Hadoop / Hive / Spark / HBase / 前端）。

## 项目目录

| 目录 | 内容 |
|------|------|
| [`week02_cluster_demo`](./week02_cluster_demo) | Week2 跨集群全链路演示（HDFS → 聚合 → HBase 写入流水线） |
| [`week03_ecommerce`](./week03_ecommerce) | Week3/Week4 电商数据分析（数据采集 → Spark 清洗 → Parquet → Spark SQL） |
| [`week05_hive`](./week05_hive) | Week5 Hive 4.0.1 基础实验（建库建表、数据加载、分组统计） |
| [`spark_practice`](./spark_practice) | PySpark 练习（JOIN、分组聚合、窗口函数、SQL 方式） |
| [`day04_hbase_python`](./day04_hbase_python) | Python HappyBase 连接 HBase 的 CRUD 演示 |
| [`day05_shell_etl`](./day05_shell_etl) | Day5 Spark ETL shell 工作流 |
| [`week1`](./week1) | Week1 练习（Python 连接 HDFS 流水线） |
| [`weather-site`](./weather-site) | 天气查询网站（Open-Meteo API，纯前端） |

## Week04 运行方式（week03_ecommerce）

```bash
cd week03_ecommerce

# 原始数据检查
/home/oyanx/soft/spark/bin/spark-submit \
--master spark://main:7077 \
scripts/inspect_raw_spark.py

# ETL 清洗
/home/oyanx/soft/spark/bin/spark-submit \
--master spark://main:7077 \
scripts/clean_online_retail.py

# Quality Gate
/home/oyanx/soft/spark/bin/spark-submit \
--master spark://main:7077 \
scripts/validate_clean_data.py

# Parquet 写入
/home/oyanx/soft/spark/bin/spark-submit \
--master spark://main:7077 \
scripts/write_parquet.py

# Spark SQL 分析
/home/oyanx/soft/spark/bin/spark-submit \
--master spark://main:7077 \
scripts/day06_spark_sql.py
```

## 目录结构

```text
xiaoshan/
├── week02_cluster_demo/   # Week2 集群全链路演示
├── week03_ecommerce/      # Week3/Week4 电商数据分析
├── week05_hive/           # Week5 Hive 基础实验
│   ├── data/              # 实验数据
│   ├── docs/              # 学习笔记
│   └── sql/               # 实验 SQL
├── spark_practice/        # PySpark 练习
├── day04_hbase_python/    # HBase Python 演示
├── day05_shell_etl/       # Spark ETL shell 工作流
├── week1/                 # Week1 练习
├── weather-site/          # 天气网站
└── README.md
```
