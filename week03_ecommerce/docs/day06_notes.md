# 第三周 Day 6 学习记录

## 今日目标

使用Shell脚本串联电商数据采集、质量检查、HDFS上传和Spark读取验证。

## 执行节点

- 主机：main
- IP：192.168.88.133
- 用户：oyanx
- 项目：~/xiaoshan/week03_ecommerce

## 流水线步骤

1. prepare_raw.py：Excel转换为CSV
2. inspect_raw.py：生成原始数据质量报告
3. upload_hdfs.py：上传CSV和报告到HDFS
4. spark_read_check.py：Spark Standalone读取HDFS验证

## HDFS路径

- 根目录：/user/hadoop/ecommerce
- 完整CSV：/user/hadoop/ecommerce/raw/online_retail.csv
- 样例CSV：/user/hadoop/ecommerce/sample/online_retail_sample.csv
- 数据报告：/user/hadoop/ecommerce/reports/data_profile.md

## 今日完成情况

- [ ] run_week03.sh已创建
- [ ] Shell语法检查通过
- [ ] 已添加执行权限
- [ ] 脚本可以自动激活.venv
- [ ] Python依赖检查通过
- [ ] 四个Python脚本语法检查通过
- [ ] Excel转换成功
- [ ] 数据质量检查成功
- [ ] WebHDFS上传成功
- [ ] HDFS目标文件存在
- [ ] fs.defaultFS获取成功
- [ ] INPUT_PATH不为空
- [ ] Spark Standalone提交成功
- [ ] 实际Master为spark://main:7077
- [ ] Spark Worker参与执行
- [ ] Spark记录数与Day 3一致
- [ ] 流水线退出码为0
- [ ] 日志文件生成成功
- [ ] 脚本重复执行仍然成功

## 实际结果

- HDFS URI：
- Spark输入路径：
- Application ID：
- Spark记录数：
- Spark分区数：
- 日志文件：
- 脚本退出码：

## 今日理解

1. Shell可以将多个独立脚本组织成统一的数据流水线。
2. set -e可以防止上一步失败后继续执行。
3. set -u可以发现变量为空或变量名写错。
4. pipefail可以正确识别管道左侧命令失败。
5. HDFS URI必须从真实fs.defaultFS配置获取。
6. 本项目统一使用/user/hadoop/ecommerce目录。
7. Spark脚本必须由spark-submit提交到Standalone集群。
8. 日志必须同时保存标准输出和错误输出。

## 遇到的问题

在这里记录实际报错、原因和解决方法。

## 明日任务

整理第三周验收文档、完善README并提交GitHub。
