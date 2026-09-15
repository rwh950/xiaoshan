-- Week5 Day1 Hive 基础实验

CREATE DATABASE week05;

USE week05;

CREATE TABLE orders (
    id INT,
    username STRING,
    amount DOUBLE,
    city STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA INPATH
'/user/hadoop/week05/raw/orders.csv'
INTO TABLE orders;

SELECT * FROM orders;

SELECT *
FROM orders
WHERE amount > 150;

SELECT username, SUM(amount)
FROM orders
GROUP BY username;

SELECT city, SUM(amount)
FROM orders
GROUP BY city;

SELECT city, COUNT(*)
FROM orders
GROUP BY city;

SELECT
    city,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount
FROM orders
GROUP BY city
ORDER BY total_amount DESC;
