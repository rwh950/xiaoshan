# Day 04：Python 操作 HBase

## 项目说明

在 Ubuntu 主节点上使用 Python 和 HappyBase，通过 HBase Thrift Server
连接 CentOS 虚拟机中的 HBase 单机环境。

## 环境架构

```text
Ubuntu main
Python + HappyBase
       |
       | TCP 9090
       v
CentOS HBase
Thrift Server + HBase
