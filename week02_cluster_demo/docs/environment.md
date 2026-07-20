# 第二周集群环境记录

## 1. Hadoop完全分布式集群

| 主机 | IP | 作用 |
|---|---|---|
| master | 192.168.88.130 | NameNode、ResourceManager |
| slave1 | 192.168.88.131 | DataNode、NodeManager |
| slave2 | 192.168.88.132 | DataNode、NodeManager |

- Hadoop用户：hadoop
- HADOOP_HOME：待填写
- Hadoop版本：待填写
- HDFS RPC地址：待填写
- NameNode WebHDFS地址：待填写
- DataNode Web端口：待填写
- WebHDFS是否启用：待填写
- Live DataNode数量：待填写

## 2. Spark Standalone集群

| 主机 | IP | 作用 |
|---|---|---|
| main | 192.168.88.133 | Spark Master、Python开发节点 |
| spark-slave | 192.168.88.134 | Spark Worker |

- 开发用户：oyanx
- Python项目运行节点：main
- Spark Master地址：待填写
- Spark版本：待填写

## 3. HBase单机环境

| 主机 | IP | 作用 |
|---|---|---|
| hbase01 | 192.168.88.151 | HBase单机 |

- HBase用户：root
- HBASE_HOME：待填写
- HBase版本：待填写
- HBase Thrift端口：9090
- Thrift状态：尚未启动或尚未检查

## 4. 网络检查

- main访问master：待填写
- main访问slave1：待填写
- main访问slave2：待填写
- main访问hbase01：待填写
- main访问NameNode WebHDFS：待填写
- main访问slave1 DataNode端口：待填写
- main访问slave2 DataNode端口：待填写

## 5. 第一天结论

- Hadoop集群是否正常：待填写
- WebHDFS链路是否正常：待填写
- Spark集群是否正常：待填写
- HBase网络是否正常：待填写
- 是否可以进入第2天Python连接HDFS：待填写
