# 第三周项目验收记录

## 一、项目基本信息

* 项目名称：电商订单大数据分析项目
* 项目目录：`~/xiaoshan/week03_ecommerce`
* 开发节点：`main`
* 开发用户：`oyanx`
* Git仓库：`rwh950/xiaoshan`
* Git分支：`master`

## 二、集群信息

### Hadoop集群

* NameNode：`master / 192.168.88.130`
* DataNode：`slave1 / 192.168.88.131`
* DataNode：`slave2 / 192.168.88.132`
* HDFS项目目录：`/user/hadoop/ecommerce`

### Spark集群

* Spark Master：`main / 192.168.88.133`
* Spark Master URL：`spark://main:7077`
* Spark Worker：`slave / 192.168.88.134`
* 提交模式：`client`

## 三、数据集信息

* 数据集名称：Online Retail
* 原始格式：XLSX
* 转换格式：UTF-8 CSV
* 核心字段数：8
* 完整记录数：以 `docs/data_profile.md` 的实际结果为准
* GitHub样例记录数：1000

## 四、代码验收

* [x] `scripts/check_source.py` 可以检查原始Excel
* [x] `scripts/prepare_raw.py` 可以生成完整CSV
* [x] `scripts/prepare_raw.py` 可以生成1000行样例
* [x] `scripts/inspect_raw.py` 可以生成质量报告
* [x] `scripts/upload_hdfs.py` 可以上传HDFS
* [x] `scripts/spark_read_check.py` 可以提交Spark任务
* [x] `run_week03.sh` 可以串联完整流程
* [x] 所有Python脚本通过语法检查
* [x] Shell脚本通过语法检查
* [x] Shell脚本具有执行权限

## 五、数据质量验收

* [x] 已统计完整记录数
* [x] 已统计字段数量
* [x] 已统计字段空值数量
* [x] 已统计字段空值比例
* [x] 已统计完全重复记录
* [x] 已统计退款或取消记录
* [x] 已统计数量异常
* [x] 已统计单价异常
* [x] 已统计交易时间范围
* [x] 已统计国家或地区数量
* [x] 原始数据未被检查脚本修改

## 六、HDFS验收

* [x] 完整CSV已上传到 `/user/hadoop/ecommerce/raw`
* [x] 样例CSV已上传到 `/user/hadoop/ecommerce/sample`
* [x] 数据质量报告已上传到 `/user/hadoop/ecommerce/reports`
* [x] 本地文件与HDFS文件大小一致
* [x] HDFS文件可以正常读取
* [x] HDFS样例CSV共有1001行文本
* [x] HDFS文件状态为HEALTHY
* [x] 上传脚本可以重复执行

## 七、Spark验收

* [x] Spark Master正常运行
* [x] Spark Worker正常运行
* [x] Spark Worker可以访问Hadoop集群
* [x] 实际Master为 `spark://main:7077`
* [x] Spark Application ID成功生成
* [x] Spark可以读取HDFS完整CSV
* [x] Spark读取字段数为8
* [x] Spark记录数与数据质量报告一致
* [x] Spark可以打印Schema
* [x] Spark可以显示样例数据
* [x] Spark可以统计字段空值
* [x] Spark Worker参与计算
* [x] Spark任务状态为FINISHED

## 八、自动化验收

* [x] `run_week03.sh` 自动激活虚拟环境
* [x] 自动检查Python依赖
* [x] 自动检查脚本语法
* [x] 自动执行Excel转CSV
* [x] 自动执行数据质量检查
* [x] 自动上传HDFS
* [x] 自动获取真实 `fs.defaultFS`
* [x] 自动构造Spark HDFS输入路径
* [x] 自动提交Spark Standalone任务
* [x] 自动保存标准输出和错误日志
* [x] 流水线失败时可以输出失败命令
* [x] 流水线成功退出码为0
* [x] 流水线可以重复运行

## 九、Git与GitHub验收

* [x] `.venv` 已加入 `.gitignore`
* [x] 原始XLSX已加入 `.gitignore`
* [x] 完整CSV已加入 `.gitignore`
* [x] 运行日志已加入 `.gitignore`
* [x] Python缓存已加入 `.gitignore`
* [x] Shell备份文件已加入 `.gitignore`
* [x] 1000行样例数据可以提交
* [x] Python脚本可以提交
* [x] Shell脚本可以提交
* [x] 数据质量报告可以提交
* [x] README已经完善
* [x] 暂存区不包含大文件
* [x] 项目已推送到GitHub

## 十、本周产出

1. 一份可复现的电商原始数据采集流程。
2. 一套Excel转CSV的Python程序。
3. 一份原始数据质量分析报告。
4. 一套WebHDFS上传程序。
5. 一个Spark Standalone读取验证程序。
6. 一个完整Shell自动化流水线。
7. 一份项目README。
8. 一份第三周验收记录。
9. 一份可供第四周继续处理的HDFS原始数据。

## 十一、第四周计划

1. 使用明确的PySpark Schema读取CSV。
2. 转换订单时间字段。
3. 删除完全重复记录并记录数量。
4. 标记退款和取消订单。
5. 处理数量小于等于0的记录。
6. 处理单价小于等于0的记录。
7. 区分销售分析数据与客户分析数据。
8. 计算订单金额。
9. 将清洗结果写入HDFS新目录。
10. 不覆盖第三周原始数据。
