from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, count, max, min, broadcast, col, row_number, rank, dense_rank, lag, lead, round as spark_round
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Day2_Practice") \
    .master("spark://main:7077") \
    .getOrCreate()

# ===== 造数据 =====
students_data = [
    ("S001", "张三", "计算机1班"),
    ("S002", "李四", "计算机1班"),
    ("S003", "王五", "计算机2班"),
    ("S004", "赵六", "计算机2班"),
    ("S005", "钱七", "计算机1班"),
]
scores_data = [
    ("S001", "SQL", 85), ("S001", "Spark", 92), ("S001", "Hadoop", 88),
    ("S002", "SQL", 76), ("S002", "Spark", 81), ("S002", "Hadoop", 79),
    ("S003", "SQL", 93), ("S003", "Hadoop", 90),
    ("S004", "SQL", 58), ("S004", "Spark", 62), ("S004", "Hadoop", 55),
]
students = spark.createDataFrame(students_data, ["学号", "姓名", "班级"])
scores = spark.createDataFrame(scores_data, ["学号", "科目", "分数"])

# ===== 1. JOIN =====
print("=== INNER JOIN ===")
students.join(scores, "学号", "inner").show()
print("=== LEFT JOIN ===")
students.join(scores, "学号", "left").show()

# ===== 2. 分组聚合 =====
print("=== 每门课统计 ===")
scores.groupBy("科目").agg(
    spark_round(avg("分数"), 1).alias("平均分"),
    max("分数").alias("最高分"),
    count("*").alias("人数")
).show()

# ===== 3. 窗口函数 =====
window_spec = Window.partitionBy("科目").orderBy(scores["分数"].desc())
print("=== 每门课排名 ===")
scores.withColumn("排名", row_number().over(window_spec)).show()

student_window = Window.partitionBy("学号").orderBy("科目")
print("=== LAG/LEAD ===")
scores.withColumn("上一科", lag("分数", 1).over(student_window)) \
      .withColumn("下一科", lead("分数", 1).over(student_window)).show()

# ===== 4. SQL方式 =====
students.createOrReplaceTempView("students")
scores.createOrReplaceTempView("scores")
print("=== SQL：学生成绩总览 ===")
spark.sql("""
    SELECT s.`姓名`, s.`班级`,
           COALESCE(SUM(sc.`分数`), 0) AS `总分`,
           ROUND(COALESCE(AVG(sc.`分数`), 0), 1) AS `平均分`
    FROM students s
    LEFT JOIN scores sc ON s.`学号` = sc.`学号`
    GROUP BY s.`学号`, s.`姓名`, s.`班级`
    ORDER BY `总分` DESC
""").show()

spark.stop()
