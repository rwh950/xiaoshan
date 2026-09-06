# 电商订单大数据分析项目

## 一、项目简介

本项目基于 Online Retail 电商交易数据，在自建 Hadoop、Spark 和 HBase 实验环境中实现端到端数据处理。

当前已经完成第三周的数据采集阶段，包括：

* 原始 Excel 文件检查
* Excel 转 UTF-8 CSV
* 字段名称标准化
* 原始数据质量分析
* WebHDFS 上传
* Spark Standalone 集群读取验证
* Shell 自动化流水线
* Git 与 GitHub 项目管理

完整链路规划：

```text
公开数据集
→ Python格式转换
→ HDFS原始数据层
→ PySpark清洗
→ HBase/Hive入库
→ Spark SQL分析
→ Tableau可视化
```

## 二、实验环境

### Hadoop 完全分布式集群

| 节点     | IP             | 作用                       |
| ------ | -------------- | ------------------------ |
| master | 192.168.88.130 | NameNode、ResourceManager |
| slave1 | 192.168.88.131 | DataNode、NodeManager     |
| slave2 | 192.168.88.132 | DataNode、NodeManager     |

### Spark Standalone 集群

| 节点    | IP             | 作用                       |
| ----- | -------------- | ------------------------ |
| main  | 192.168.88.133 | Spark Master、开发节点、Driver |
| slave | 192.168.88.134 | Spark Worker、Executor    |

### HBase

| 节点      | IP             | 作用           |
| ------- | -------------- | ------------ |
| hbase01 | 192.168.88.151 | HBase单节点实验环境 |

## 三、数据字段

| 字段           | 含义    |
| ------------ | ----- |
| invoice_no   | 订单编号  |
| stock_code   | 商品编号  |
| description  | 商品描述  |
| quantity     | 商品数量  |
| invoice_date | 交易时间  |
| unit_price   | 商品单价  |
| customer_id  | 客户编号  |
| country      | 国家或地区 |

## 四、项目目录

```text
week03_ecommerce/
├── data/
│   ├── source/        # 原始文件，不上传GitHub
│   ├── raw/           # 完整CSV，不上传GitHub
│   └── sample/        # 1000行样例数据
├── docs/              # 学习记录、质量报告和验收文档
├── logs/              # 运行日志，不上传GitHub
├── scripts/           # Python处理脚本
├── .gitignore
├── README.md
├── requirements.txt
└── run_week03.sh
```

## 五、主要脚本

| 文件                          | 作用                         |
| --------------------------- | -------------------------- |
| scripts/check_source.py     | 检查原始Excel文件和字段结构           |
| scripts/prepare_raw.py      | 将Excel转换为UTF-8 CSV         |
| scripts/inspect_raw.py      | 检查空值、重复、退款和异常数据            |
| scripts/upload_hdfs.py      | 使用WebHDFS上传数据和报告           |
| scripts/spark_read_check.py | 使用Spark Standalone读取HDFS数据 |
| run_week03.sh               | 串联第三周完整数据采集流程              |

## 六、HDFS目录

```text
/user/hadoop/ecommerce/
├── raw/
│   └── online_retail.csv
├── sample/
│   └── online_retail_sample.csv
└── reports/
    └── data_profile.md
```

## 七、运行方法

所有代码在 Spark 主节点 `main` 上执行。

### 1. 进入项目

```bash
cd ~/xiaoshan/week03_ecommerce
```

### 2. 创建并激活虚拟环境

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. 安装依赖

```bash
python3 -m pip install -r requirements.txt
```

### 4. 运行完整流水线

```bash
chmod +x run_week03.sh
./run_week03.sh
```

### 5. 查看最新日志

```bash
less logs/week03_latest.log
```

## 八、第三周已完成内容

* [x] 创建电商项目目录
* [x] 获取 Online Retail 原始数据
* [x] 检查原始 Excel 文件
* [x] 转换为 UTF-8 CSV
* [x] 统一字段名称
* [x] 生成 1000 行样例
* [x] 检查字段空值
* [x] 检查完全重复记录
* [x] 检查退款和取消订单
* [x] 检查数量与价格异常
* [x] 生成数据质量报告
* [x] 上传完整 CSV 到 HDFS
* [x] 上传样例数据到 HDFS
* [x] 上传质量报告到 HDFS
* [x] Spark Standalone 读取 HDFS 数据
* [x] Spark Worker 参与任务执行
* [x] Shell 串联完整流程
* [x] 保存运行日志
* [x] 使用 Git 管理项目

## 九、数据处理原则

* 原始 Excel 文件永久保留，不直接修改。
* `data/raw` 只进行格式和字段统一，不进行业务清洗。
* 退款、负数数量、零价格和缺失客户编号暂时保留。
* 完整数据和日志不上传 GitHub。
* GitHub 只保存代码、文档和小型样例数据。
* 第四周使用 PySpark 完成正式数据清洗。
* 清洗结果写入新的 HDFS 目录，不覆盖原始数据。

## 十、下一阶段

第四周计划：

```text
明确Schema
→ 日期类型转换
→ 空值处理
→ 重复记录处理
→ 退款订单标记
→ 数量和价格异常处理
→ 计算订单金额
→ 清洗结果写回HDFS
```


## Week04 Day05 - Parquet Output

- Raw records: 541909
- Duplicate records: 5268
- Clean records: 536641
- Output format: Parquet
- Compression: Snappy
- Partition columns: invoice_year, invoice_month

### Order Types

- SALE: 524878
- RETURN: 9251
- ADJUSTMENT: 1338
- ZERO_PRICE: 1174

### Output Path

hdfs://master:9000/user/hadoop/week03/clean

# Week04 ETL Pipeline

## 1. 项目目标

使用 Spark 对 Online Retail 电商交易数据进行离线 ETL 处理，
完成原始数据读取、Schema 标准化、重复数据处理、业务分类、
数据质量检查、Parquet 分区存储以及 Spark SQL 分析。

## 2. 数据链路

Online Retail.xlsx
    ↓
CSV
    ↓
HDFS Raw
    ↓
Spark ETL
    ↓
去重 + 业务分类
    ↓
Quality Gate
    ↓
Parquet
    ↓
HDFS Clean
    ↓
Spark SQL Analysis

## 3. 集群环境

| 节点 | IP | 角色 |
|---|---|---|
| master | 192.168.88.130 | Hadoop NameNode / ResourceManager |
| slave1 | 192.168.88.131 | Hadoop DataNode / NodeManager |
| slave2 | 192.168.88.132 | Hadoop DataNode / NodeManager |
| main | 192.168.88.133 | Spark Master / Python Development |
| spark-slave | 192.168.88.134 | Spark Worker |

Spark Master:

spark://main:7077

HDFS:

hdfs://master:9000

## 4. Day01-Day02 数据画像

原始数据量：

541909

字段数量：

8

Quantity < 0：

10624

InvoiceNo 以 C 开头：

9288

UnitPrice = 0：

2515

UnitPrice < 0：

2

Description 缺失：

1454

CustomerID 缺失：

135080

完全重复记录：

5268

## 5. Day03 清洗结果

去重后数据量：

536641

业务分类：

| order_type | count |
|---|---:|
| SALE | 524878 |
| RETURN | 9251 |
| ADJUSTMENT | 1338 |
| ZERO_PRICE | 1174 |
| REJECTED | 0 |

分类数量合计：

536641

## 6. Day04 Quality Gate

数据数量对账：

541909 = 5268 + 536641

分类数据对账：

524878 + 9251 + 1338 + 1174 + 0 = 536641

分类规则验证：

- SALE violation = 0
- RETURN violation = 0
- ADJUSTMENT violation = 0
- ZERO_PRICE violation = 0

其他质量检查：

- amount null = 0
- date violation = 0
- CustomerID flag violation = 0
- Description flag violation = 0
- duplicate after clean = 0

## 7. Parquet 输出

输出路径：

hdfs://master:9000/user/hadoop/week03/clean

格式：

Parquet

压缩：

Snappy

分区字段：

- invoice_year
- invoice_month

Parquet 回读数量：

536641

## 8. Spark SQL 分析

已完成：

- 数据总览
- 业务类型分布
- 正常销售核心指标
- 国家销售额 Top10
- 商品销售额 Top10
- 商品销量 Top10
- 月度销售趋势
- 小时销售趋势
- 用户销售额 Top10
- 退货统计
- 净交易金额

## 9. 核心分析结果

正常销售：

销售明细：

524878

不同订单：

19960

不同客户：

4338

不同商品：

3922

销售数量：

5572420

销售额：

10642110.80

月度销售峰值：

2011-11

销售额：

1503866.78

小时订单量峰值：

12:00

订单量：

3220

小时销售额峰值：

10:00

销售额：

1444814.77

## 10. 主要项目脚本

scripts/inspect_raw_spark.py
    原始数据检查

scripts/clean_online_retail.py
    ETL 清洗与分类

scripts/validate_clean_data.py
    数据质量验证

scripts/write_parquet.py
    Parquet 分区写入

scripts/day06_spark_sql.py
    Spark SQL 分析

sql/day06_analysis.sql
    SQL 分析脚本
