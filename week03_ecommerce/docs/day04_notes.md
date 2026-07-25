# 第三周 Day 4 学习记录

## 今日目标

使用 Python WebHDFS 客户端，将本地电商数据上传到 Hadoop 集群。

## 运行环境

- 开发节点：main
- 开发用户：oyanx
- NameNode：192.168.88.130
- WebHDFS端口：9870
- HDFS用户：hadoop
- HDFS项目目录：/user/hadoop/ecommerce

## 上传内容

- data/raw/online_retail.csv
  - HDFS：/user/hadoop/ecommerce/raw/online_retail.csv

- data/sample/online_retail_sample.csv
  - HDFS：/user/hadoop/ecommerce/sample/online_retail_sample.csv

- docs/data_profile.md
  - HDFS：/user/hadoop/ecommerce/reports/data_profile.md

## 今日完成情况

- [ ] main可以解析master、slave1、slave2
- [ ] main可以访问NameNode 9870端口
- [ ] main可以访问DataNode 9864端口
- [ ] Python hdfs客户端可以导入
- [ ] upload_hdfs.py语法检查通过
- [ ] 完整CSV上传成功
- [ ] 样例CSV上传成功
- [ ] 数据质量报告上传成功
- [ ] 本地与HDFS文件大小一致
- [ ] master可以列出HDFS文件
- [ ] HDFS中的CSV表头正确
- [ ] 样例CSV为1001行
- [ ] hdfs fsck结果为HEALTHY
- [ ] 上传脚本可以重复执行

## 今日理解

WebHDFS上传分为两个阶段：

1. 客户端先访问NameNode的9870端口。
2. NameNode选择DataNode并返回重定向地址。
3. 客户端连接DataNode的9864端口并上传数据。
4. NameNode维护文件路径和数据块元数据。
5. DataNode实际保存数据块。

## 遇到的问题

在这里记录实际报错、原因和解决方法。

## 明日任务

使用Spark Standalone集群读取HDFS中的完整CSV，检查Schema、记录数和分区数。
