# xiaoshan


## 11. Week04 运行方式

### 原始数据检查

/home/oyanx/soft/spark/bin/spark-submit \
--master spark://main:7077 \
scripts/inspect_raw_spark.py

### ETL 清洗

/home/oyanx/soft/spark/bin/spark-submit \
--master spark://main:7077 \
scripts/clean_online_retail.py

### Quality Gate

/home/oyanx/soft/spark/bin/spark-submit \
--master spark://main:7077 \
scripts/validate_clean_data.py

### Parquet 写入

/home/oyanx/soft/spark/bin/spark-submit \
--master spark://main:7077 \
scripts/write_parquet.py

### Spark SQL 分析

/home/oyanx/soft/spark/bin/spark-submit \
--master spark://main:7077 \
scripts/day06_spark_sql.py
