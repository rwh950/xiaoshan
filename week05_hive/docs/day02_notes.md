# Week5 Day2：Hive DDL 与 DML 实验记录

## 1. 实验目标

- 掌握 Hive 内部表与外部表的创建方式。
- 掌握 LOAD DATA 导入 CSV 数据。
- 理解 INSERT INTO 与 INSERT OVERWRITE 的区别。
- 验证 DROP TABLE 对表元数据和数据文件的影响。
- 掌握 ALTER TABLE ADD COLUMNS。

## 2. 实验环境

- 操作系统：Ubuntu 22.04
- Hadoop：3.3.6
- Hive：4.0.1
- HDFS 地址：hdfs://master:9000
- 数据库：week05
- 实验目录：/home/hadoop/week05_hive

## 3. 内部表实验

### 实验操作

创建内部表 orders_managed，并查看表结构和详细属性。

### 实际结果

- 表是否创建成功：待填写
- Table Type：待填写
- Location：待填写
- 数据查询结果：待填写

## 4. 外部表实验

### 实验操作

创建外部表 orders_external，并指定 HDFS 存储目录。

### 实际结果

- 表是否创建成功：待填写
- Table Type：待填写
- Location：待填写
- 数据查询结果：待填写

## 5. LOAD DATA 实验

### 实验操作

使用 LOAD DATA LOCAL INPATH 将本地 CSV 文件导入目标表。

### 实际结果

- 导入的目标表：待填写
- 查询记录数：待填写
- HDFS 数据目录：待填写

## 6. INSERT INTO 实验

### 实验操作

向 orders_managed 追加一条订单记录。

### 实际结果

- 执行是否成功：待填写
- 追加前记录数：待填写
- 追加后记录数：待填写

### 实验结论

INSERT INTO 用于向目标表追加数据。

## 7. INSERT OVERWRITE 实验

### 实验操作

筛选 city 为 Beijing 的记录，并将查询结果覆盖写入 orders_managed。

### 实际结果

- 覆盖前记录数：待填写
- 覆盖后记录数：待填写
- 筛选条件：city = 'Beijing'

### 实验结论

INSERT OVERWRITE 会用查询结果替换目标数据，执行前需要确认目标表及覆盖范围。

## 8. DROP TABLE 实验

### 实验操作

创建临时内部表 managed_drop_demo，检查其 HDFS 目录，然后删除该表并再次检查目录。

### 实际结果

- 删除前目录是否存在：待填写
- 删除后目录是否存在：待填写

### 实验结论

内部表删除时，通常会删除表元数据和 Hive 管理的数据文件；具体行为应以实际表属性和环境验证为准。

## 9. ALTER TABLE 实验

### 实验操作

为 orders_external 添加 remark STRING 字段。

### 实际结果

- 字段是否添加成功：待填写
- 旧数据的 remark 字段值：待填写

### 实验结论

ALTER TABLE ADD COLUMNS 用于扩展表结构，不会自动为原始 CSV 文件补充新字段。

## 10. 问题与总结

### 遇到的问题

待填写实际错误信息或填写“本次实验未遇到问题”。

### 解决方法

待填写实际解决过程。

### 学习总结

本次实验学习了 Hive 内部表、外部表、数据加载、追加写入、覆盖写入、删除表和修改表结构等操作，并通过查询结果与 HDFS 目录检查验证操作效果。
