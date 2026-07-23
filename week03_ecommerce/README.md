# 电商订单大数据分析项目

## 项目目标

在现有 Hadoop、Spark、HBase 环境上完成一条端到端的数据处理链路：

公开数据集 → Python格式转换 → HDFS存储 → PySpark清洗
→ HBase/Hive入库 → Spark SQL分析 → Tableau可视化

## 当前进度

- [x] 创建第三周项目目录
- [x] 获取 Online Retail 原始数据集
- [x] 检查原始文件格式
- [x] 检查 Excel 字段结构
- [ ] 转换为 UTF-8 CSV
- [ ] 上传 HDFS
- [ ] Spark 集群读取验证

## 项目环境

- Spark开发节点：main，192.168.88.133
- Spark Worker：slave，192.168.88.134
- Hadoop NameNode：master，192.168.88.130
- Hadoop Worker：slave1、slave2

## 数据目录

- data/source：官方原始文件
- data/raw：格式统一后的原始数据
- data/sample：可上传GitHub的小型样例
