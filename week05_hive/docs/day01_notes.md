# Week5 Day1：Hive 基础

## 环境

- Ubuntu 22.04
- Java 8
- Hadoop 3.3.6
- Hive 4.0.1
- HDFS：hdfs://master:9000
- HiveServer2：master:10000
- Metastore：Embedded Derby

## 今日完成

- Hive 4.0.1 安装
- Hive Metastore 初始化
- HiveServer2 启动
- Beeline 连接
- 创建 week05 数据库
- 创建 orders 表
- HDFS 数据加载
- SELECT
- WHERE
- GROUP BY
- SUM
- COUNT
- ORDER BY

## 数据流程

CSV
↓
HDFS
↓
Hive Table
↓
Hive SQL
↓
聚合统计

## orders 表结构

| 字段 | 类型 |
|---|---|
| id | INT |
| username | STRING |
| amount | DOUBLE |
| city | STRING |

## 实验结果

### 城市销售额

- Beijing：330.5
- Shanghai：320.0
- Shenzhen：300.0

### 城市订单量

- Beijing：3
- Shanghai：2
- Shenzhen：1

## 核心概念

HDFS：
保存实际数据。

Metastore：
保存 Hive 元数据。

Hive：
提供数据仓库和 SQL 查询能力。

HiveServer2：
提供 Hive 查询服务。

Beeline：
连接 HiveServer2 的命令行客户端。
