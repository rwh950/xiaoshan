# Week02 Python + HDFS + HBase 数据流水线

## 一、项目简介

本项目用于演示 Python 程序如何连接 Hadoop HDFS 和 HBase，并通过 Shell 脚本完成自动化数据处理。

执行一条命令即可完成：

```text
本地 CSV
→ 上传 HDFS
→ Python 读取订单
→ 按用户聚合
→ 结果写回 HDFS
→ 聚合数据写入 HBase
→ 自动检查结果
→ 保存运行日志
```

## 二、实验环境

### 1. Hadoop 完全分布式集群

| 主机 | IP | 作用 |
|---|---|---|
| master | 192.168.88.130 | NameNode、ResourceManager |
| slave1 | 192.168.88.131 | DataNode、NodeManager |
| slave2 | 192.168.88.132 | DataNode、NodeManager |

### 2. Spark Standalone 集群

| 主机 | IP | 作用 |
|---|---|---|
| main | 192.168.88.133 | Spark Master、Python 开发节点 |
| spark-slave | 192.168.88.134 | Spark Worker |

### 3. HBase 环境

| 主机 | IP | 作用 |
|---|---|---|
| hbase01 | 192.168.88.151 | HBase 单机、Thrift Server |

## 三、技术栈

- Python 3
- WebHDFS
- hdfs Python 客户端
- HappyBase
- HBase Thrift 1
- Bash Shell
- Hadoop HDFS
- HBase
- Git

## 四、项目结构

```text
week02_cluster_demo/
├── config.example.env
├── config.env
├── data/
│   └── orders.csv
├── docs/
├── logs/
├── output/
│   └── user_summary_20260718.csv
├── shell/
│   ├── check_environment.sh
│   └── run_pipeline.sh
├── src/
│   ├── aggregate_orders.py
│   ├── hbase_writer.py
│   └── hdfs_demo.py
├── .gitignore
├── README.md
└── requirements.txt
```

## 五、数据流程

```text
data/orders.csv
       |
       v
HDFS /user/hadoop/week02/raw/YYYYMMDD/orders.csv
       |
       v
src/aggregate_orders.py
       |
       +--> output/user_summary_YYYYMMDD.csv
       |
       +--> HDFS /user/hadoop/week02/result/YYYYMMDD/user_summary.csv
       |
       v
src/hbase_writer.py
       |
       v
HBase 表 week02_user_stat
```

## 六、HBase 表设计

表名：

```text
week02_user_stat
```

列族：

```text
cf
```

RowKey 格式：

```text
业务日期#用户ID
```

示例：

```text
20260718#u001
```

主要列：

```text
cf:user_id
cf:order_count
cf:total_amount
cf:biz_date
cf:update_time
```

字段说明：

| 字段 | 说明 |
|---|---|
| cf:user_id | 用户编号 |
| cf:order_count | 用户订单数量 |
| cf:total_amount | 用户订单总金额 |
| cf:biz_date | 业务日期 |
| cf:update_time | 数据写入或更新时间 |

## 七、安装 Python 依赖

在项目目录中创建并激活虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
```

安装依赖：

```bash
python -m pip install -r requirements.txt
```

检查依赖：

```bash
python -c "import hdfs, happybase, thrift; print('Python依赖正常')"
```

## 八、配置项目环境

复制示例配置：

```bash
cp config.example.env config.env
```

根据实际集群地址修改：

```bash
vim config.env
```

主要配置项：

```bash
HDFS_URL=http://master:9870
HDFS_USER=hadoop
HDFS_BASE=/user/hadoop/week02

DATANODE_WEB_PORT=9864
HADOOP_WORKERS="slave1 slave2"

HBASE_HOST=192.168.88.151
HBASE_PORT=9090
HBASE_TABLE=week02_user_stat
HBASE_FAMILY=cf
```

`config.env` 保存具体环境地址，通常不提交到 Git 仓库。

## 九、检查运行环境

执行：

```bash
bash shell/check_environment.sh 2026-07-18
```

检查内容包括：

- 项目文件
- Python 虚拟环境和依赖
- 主机名解析
- NameNode WebHDFS
- DataNode Web 端口
- HDFS 数据文件
- HBase Thrift
- HBase 表
- Spark Master 进程

## 十、运行完整流水线

执行：

```bash
bash shell/run_pipeline.sh \
  data/orders.csv \
  2026-07-18
```

流水线将自动完成：

1. 检查基础环境
2. 上传本地 CSV 到 HDFS
3. 读取订单并按用户聚合
4. 将结果写入 HDFS
5. 将聚合结果写入 HBase
6. 自动检查处理结果
7. 保存运行日志

## 十一、聚合结果示例

```csv
user_id,order_count,total_amount
u001,3,4167.00
u002,2,69.00
u003,1,299.00
```

## 十二、HDFS 目录

原始数据：

```text
/user/hadoop/week02/raw/20260718/orders.csv
```

聚合结果：

```text
/user/hadoop/week02/result/20260718/user_summary.csv
```

查看结果：

```bash
hdfs dfs -cat \
  /user/hadoop/week02/result/20260718/user_summary.csv
```

## 十三、查询 HBase 数据

进入 HBase Shell：

```bash
hbase shell
```

查看表：

```text
list
```

扫描数据：

```text
scan 'week02_user_stat'
```

退出：

```text
exit
```

## 十四、日志文件

每次运行流水线都会在 `logs` 目录保存日志：

```text
logs/pipeline_业务日期_运行时间.log
```

示例：

```text
logs/pipeline_20260718_20260722_131154.log
```

## 十五、项目特点

- 使用 Python 连接 WebHDFS
- 使用 HappyBase 连接 HBase Thrift
- 支持业务日期参数
- 支持 HDFS 双副本存储
- 支持自动创建 HDFS 目录
- 支持数据上传、下载和哈希校验
- 支持订单聚合和结果回读校验
- 支持 HBase 批量写入
- 支持 Shell 一键执行
- 支持环境检查和错误定位
- 支持运行日志保存

## 十六、注意事项

1. Hadoop、Spark 和 HBase 服务需要提前启动。
2. HBase Thrift Server 必须监听 9090 端口。
3. `main` 节点必须能够访问 NameNode、DataNode 和 HBase。
4. 不要将真实环境的 `config.env` 提交到公开仓库。
5. 不要随意执行 `hdfs namenode -format`，避免丢失 HDFS 元数据。
