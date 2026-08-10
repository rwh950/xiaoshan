# Week04 Online Retail 数据清洗规则

## 1. 原始数据

HDFS 输入路径：

`hdfs://master:9000/user/hadoop/week03/raw/online_retail.csv`

Spark 实际读取字段：

- invoice_no
- stock_code
- description
- quantity
- invoice_date
- unit_price
- customer_id
- country

## 2. Day01 数据检查

检查内容：

- 原始数据总行数
- Spark Schema
- 每个字段空值数量
- quantity 最小值和最大值
- unit_price 最小值和最大值
- country 去重数量
- quantity = 0 数据数量
- quantity < 0 数据数量
- unit_price = 0 数据数量
- unit_price < 0 数据数量
- invoice_no 以 C 开头的数据数量

## 3. 初步发现

Day01 暂不直接删除异常记录。

重点关注：

1. customer_id 空值
2. quantity 负数
3. unit_price 为 0 或负数
4. invoice_no 以 C 开头
5. description 空值
6. invoice_date 字段类型

这些数据将在后续清洗步骤中进一步分类。

## 4. 当前原则

- 不直接删除 customer_id 为空的记录
- 不直接删除 quantity 为负数的记录
- 不直接删除 invoice_no 以 C 开头的记录
- Day01 只建立原始数据质量基线
- 后续根据业务含义划分销售、退货/取消和异常记录



## Day01 原始数据质量基线

原始数据总行数：541909

字段数量：8

Country 去重数量：38

初步异常统计：

- Quantity = 0：0
- Quantity < 0：10624
- UnitPrice = 0：2515
- UnitPrice < 0：2
- InvoiceNo 以 C 开头：9288

### 初步判断

1. Quantity 负数记录共 10624 条，需要进一步判断其与取消订单之间的关系。
2. InvoiceNo 以 C 开头的记录共 9288 条，不能简单等同于所有 Quantity 负数记录。
3. UnitPrice 为 0 的记录共 2515 条，暂不删除，需要结合 StockCode、Description 等字段分析业务含义。
4. UnitPrice 小于 0 的记录仅 2 条，后续进行逐条检查。
5. Quantity 不存在等于 0 的记录。
6. Day01 暂不进行数据删除，仅建立数据质量基线。

### Day02 最终交叉验证

非 C 开头负数量记录：

- UnitPrice = 0：1336
- UnitPrice > 0：0
- UnitPrice < 0：0

说明全部 1336 条非 C 开头负数量记录同时为零价格记录，
结合 Description 中的 check、damages 等内容，
后续统一作为 ADJUSTMENT 类型处理。

零价格记录：

- UnitPrice = 0 总数：2515
- Quantity < 0：1336
- Quantity > 0：1179
- C 开头订单：0

其中负数量零价格记录进入 ADJUSTMENT，
剩余 1179 条正数量零价格记录进入 ZERO_PRICE，
不直接作为正常销售记录参与销售额计算。

Description 缺失：

- Description NULL + UnitPrice = 0：1454
- Description NULL + UnitPrice != 0：0

说明全部 Description 缺失记录同时属于零价格数据，
暂不删除，根据其他业务字段分类保存。

负价格记录：

共 2 条，Description 均为 Adjust bad debt，
属于坏账调整，归入 ADJUSTMENT。

### Day03 最终分类规则

分类优先级：

1. REJECTED
2. RETURN
3. ADJUSTMENT
4. ZERO_PRICE
5. SALE

RETURN：
InvoiceNo 以 C 开头。

ADJUSTMENT：
非 C 开头且 Quantity < 0，
或者 UnitPrice < 0。

ZERO_PRICE：
非 C 开头、Quantity > 0 且 UnitPrice = 0。

SALE：
非 C 开头、Quantity > 0 且 UnitPrice > 0。

CustomerID 缺失和 Description 缺失暂不作为删除条件。
