# Week04 项目验收报告

## 一、项目名称

Online Retail Spark ETL Pipeline

## 二、运行环境

- Spark 3.5.8
- Hadoop / HDFS
- Spark Standalone
- Python 3
- Git / GitHub

Spark Master：

spark://main:7077

HDFS：

hdfs://master:9000

开发节点：

192.168.88.133

Worker：

192.168.88.134

## 三、Day01 原始数据检查

- [x] HDFS 原始数据读取成功
- [x] Spark Standalone 提交成功
- [x] 原始数据行数 = 541909
- [x] 字段数量 = 8
- [x] Country 数量 = 38
- [x] Quantity < 0 = 10624
- [x] UnitPrice = 0 = 2515
- [x] InvoiceNo C 开头 = 9288

## 四、Day02 Schema 标准化

标准字段：

- invoice_no: string
- stock_code: string
- description: string
- quantity: integer
- invoice_date: timestamp
- unit_price: double
- customer_id: string
- country: string

类型转换失败：

- Quantity = 0
- UnitPrice = 0
- InvoiceDate = 0

字段缺失：

- invoice_no = 0
- stock_code = 0
- description = 1454
- customer_id = 135080
- country = 0

业务异常：

- C 开头 + Quantity < 0 = 9288
- 非 C 开头 + Quantity < 0 = 1336
- UnitPrice = 0 = 2515
- UnitPrice < 0 = 2

## 五、Day03 ETL 清洗

原始数据：

541909

完全重复：

5268

去重后：

536641

### order_type

| order_type | count |
|---|---:|
| SALE | 524878 |
| RETURN | 9251 |
| ADJUSTMENT | 1338 |
| ZERO_PRICE | 1174 |
| REJECTED | 0 |

分类总数：

536641

分类对账：

PASS

## 六、Day04 Quality Gate

原始数据对账：

541909 = 5268 + 536641

PASS

业务分类规则：

- SALE violation = 0
- RETURN violation = 0
- ADJUSTMENT violation = 0
- ZERO_PRICE violation = 0

金额检查：

- amount null = 0
- SALE amount violation = 0
- RETURN amount violation = 0

日期检查：

- date violation = 0

缺失标记：

- CustomerID flag violation = 0
- Description flag violation = 0

重复检查：

- duplicate after clean = 0

最终状态：

PASS

## 七、Day05 Parquet

输出：

hdfs://master:9000/user/hadoop/week03/clean

格式：

Parquet

压缩：

Snappy

分区：

invoice_year / invoice_month

写入状态：

PASS

Parquet 回读：

536641

## 八、Day06 Spark SQL

完成：

- 数据总览
- 业务类型分析
- 正常销售指标
- 国家销售额 Top10
- 商品销售额 Top10
- 商品销量 Top10
- 月度销售趋势
- 小时销售趋势
- 用户销售额 Top10
- 退货统计
- 净交易金额

核心正常销售指标：

- 销售明细 = 524878
- 不同订单 = 19960
- 不同客户 = 4338
- 不同商品 = 3922
- 销售数量 = 5572420
- 销售额 = 10642110.80

## 九、项目结论

Week04 已完成从 HDFS Raw 数据到 Spark ETL、
Quality Gate、Parquet 分区存储和 Spark SQL 分析的完整离线数据处理流程。

最终清洗数据：

536641

最终 Parquet 数据：

536641

数据回读：

PASS
