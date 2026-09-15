# Week5 Hive 学习项目

## Week5 Day1：Hive 基础

本项目记录 Hive 4.0.1 在 Hadoop 3.3.6 环境中的基础学习与实验。

## 环境

- Java 8
- Hadoop 3.3.6
- Hive 4.0.1
- HDFS：hdfs://master:9000
- HiveServer2：master:10000
- Metastore：Embedded Derby

## Day1 数据流程

```text
orders.csv
    ↓
HDFS
    ↓
Hive
    ↓
week05.orders
    ↓
Hive SQL
    ↓
统计结果
```

## 已完成

- Hive 环境部署
- Metastore 初始化
- HiveServer2
- Beeline
- Database
- Table
- HDFS 数据加载
- SELECT / WHERE / GROUP BY / SUM / COUNT / ORDER BY

## 目录结构

```text
week05_hive/
├── data/
│   └── orders.csv
├── docs/
│   └── day01_notes.md
└── sql/
    └── day01_basic.sql
```

## 后续

Week5 Day2：Hive DDL / DML、Managed Table、External Table。
