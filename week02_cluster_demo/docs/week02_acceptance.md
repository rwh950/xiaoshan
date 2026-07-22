# 第二周项目验收记录

## 一、环境信息

- 开发节点：main
- 开发节点 IP：192.168.88.133
- Hadoop NameNode：master
- Hadoop NameNode IP：192.168.88.130
- Hadoop DataNode：slave1、slave2
- HBase 节点：192.168.88.151
- HBase Thrift 端口：9090

## 二、验收项目

- [x] main 可以访问 Hadoop NameNode
- [x] main 可以访问两个 DataNode
- [x] Python 可以连接 WebHDFS
- [x] Python 可以上传 CSV 到 HDFS
- [x] Python 可以读取 HDFS 文件
- [x] Python 可以下载 HDFS 文件
- [x] Python 可以按用户聚合订单
- [x] 聚合结果可以写回 HDFS
- [x] HappyBase 可以连接 HBase Thrift
- [x] 聚合结果可以写入 HBase
- [x] Shell 可以自动检查运行环境
- [x] Shell 可以一键执行完整流水线
- [x] 流水线可以保存完整日志
- [x] 重复运行不会产生重复 RowKey

## 三、最终结果

### 1. HDFS 原始数据

```text
/user/hadoop/week02/raw/20260718/orders.csv
```

### 2. HDFS 聚合结果

```text
/user/hadoop/week02/result/20260718/user_summary.csv
```

### 3. HBase 表

```text
week02_user_stat
```

### 4. HBase RowKey

```text
20260718#u001
20260718#u002
20260718#u003
```

### 5. 聚合结果

```csv
user_id,order_count,total_amount
u001,3,4167.00
u002,2,69.00
u003,1,299.00
```

## 四、最终运行命令

```bash
bash shell/run_pipeline.sh \
  data/orders.csv \
  2026-07-18
```

## 五、最终验收结论

第二周 Python、HDFS、HBase 和 Shell 自动化数据流水线已完成。

完整流程：

```text
本地 CSV
→ 上传到 HDFS
→ Python 读取并聚合
→ 聚合结果写回 HDFS
→ 聚合结果写入 HBase
→ Shell 自动检查和执行
→ 保存运行日志
```
