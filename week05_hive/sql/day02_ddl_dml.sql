-- Week5 Day2: Hive DDL and DML
-- 注意：本文件用于记录实验，不建议直接整份重复执行。

USE week05;

-- 1. Create managed table
CREATE TABLE IF NOT EXISTS orders_managed (
    id INT,
    username STRING,
    amount DOUBLE,
    city STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- 2. Create external table
CREATE EXTERNAL TABLE IF NOT EXISTS orders_external (
    id INT,
    username STRING,
    amount DOUBLE,
    city STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hadoop/week05/external_orders';

-- 3. Inspect table metadata
DESCRIBE orders_managed;
DESCRIBE FORMATTED orders_managed;
DESCRIBE FORMATTED orders_external;

-- 4. Load local CSV data
-- 只有确认目标表需要导入数据时，才执行下面的语句。
-- 重复导入可能产生重复记录。
-- LOAD DATA LOCAL INPATH '/home/hadoop/week05_hive/data/orders.csv'
-- INTO TABLE orders_managed;
--
-- LOAD DATA LOCAL INPATH '/home/hadoop/week05_hive/data/orders.csv'
-- INTO TABLE orders_external;

-- 5. Query imported data
SELECT * FROM orders_managed;
SELECT COUNT(*) FROM orders_managed;
SELECT * FROM orders_external;
SELECT COUNT(*) FROM orders_external;

-- 6. Append one record
-- 执行前先检查当前数据，避免重复追加。
-- INSERT INTO TABLE orders_managed
-- VALUES (7, 'chenliu', 66.00, 'Guangzhou');

-- 7. Overwrite with Beijing orders
-- 这会用查询结果替换 orders_managed 的现有数据。
-- 确认需要覆盖后才能执行。
-- INSERT OVERWRITE TABLE orders_managed
-- SELECT id, username, amount, city
-- FROM orders_managed
-- WHERE city = 'Beijing';

-- 8. Alter external table schema
-- 如果 remark 已经添加过，不要重复执行。
-- ALTER TABLE orders_external
-- ADD COLUMNS (remark STRING);

DESCRIBE orders_external;
