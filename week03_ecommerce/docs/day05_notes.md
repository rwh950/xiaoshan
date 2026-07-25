# 第三周 Day 5 学习记录

## 今日目标

使用 Spark Standalone 集群读取 Hadoop HDFS 中的完整电商订单 CSV，验证跨集群数据访问。

## 集群信息

### Hadoop

- NameNode：master，192.168.88.130
- DataNode：slave1，192.168.88.131
- DataNode：slave2，192.168.88.132
- HDFS文件：/user/hadoop/ecommerce/raw/online_retail.csv

### Spark

- Spark Master：main，192.168.88.133
- Spark Master URL：spark://main:7077
- Spark Worker：slave，192.168.88.134
- 提交模式：client

## 今日完成情况

- [ ] HDFS原始CSV存在
- [ ] HDFS文件状态为HEALTHY
- [ ] Spark Master正常运行
- [ ] Spark Worker正常运行
- [ ] Spark Web UI显示1个Alive Worker
- [ ] Spark Worker可以解析master
- [ ] Spark Worker可以解析slave1和slave2
- [ ] spark_read_check.py语法检查通过
- [ ] spark-submit提交成功
- [ ] 实际Master为spark://main:7077
- [ ] Application ID成功生成
- [ ] CSV字段数为8
- [ ] Spark记录数与Day 3一致
- [ ] Schema打印正常
- [ ] 前10条数据可以显示
- [ ] 字段空值统计成功
- [ ] Spark Worker参与计算
- [ ] Spark退出码为0
- [ ] 日志已保存

## 实际结果

- HDFS URI：
- Application ID：
- Spark版本：
- 记录数：
- 分区数：
- 默认并行度：
- Spark Master：
- Spark Worker数量：

## 今日理解

1. Spark Master负责资源调度，不保存HDFS数据。
2. Spark Worker运行Executor并执行Task。
3. Hadoop NameNode维护HDFS文件元数据。
4. Hadoop DataNode保存真实数据块。
5. Spark Worker需要能够访问NameNode和DataNode。
6. DataFrame采用惰性计算，count和show才会触发Job。
7. inferSchema适合验证，正式ETL应定义明确Schema。
8. 代码中不能写master("local[*]")，否则不会使用Standalone集群。

## 遇到的问题

在这里记录实际报错、原因和解决方法。

## 明日任务

使用Shell脚本串联Excel转换、数据检查、HDFS上传和Spark读取验证。
