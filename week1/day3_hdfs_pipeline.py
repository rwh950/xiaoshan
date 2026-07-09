"""
第3天全链路跨集群验证

Hadoop 集群: master (NameNode) + slave1,slave2 (DataNode)
Spark 集群:  main (Master) + slave (Worker)

本脚本在 Spark 集群的 main 上运行，
通过 hdfs 库和 PySpark 远程访问 Hadoop 集群的 HDFS
"""
from hdfs import InsecureClient
from pyspark.sql import SparkSession

# ===== 配置：Hadoop 集群的地址 =====
HADOOP_MASTER_IP = "192.168.88.130"
HADOOP_WEB_PORT = "9870"    # Hadoop 3.x 用 9870
HADOOP_RPC_PORT = "9000"    # core-site.xml 里 fs.defaultFS 的端口

# ===== 配置：Spark 集群的地址 =====
SPARK_MASTER_IP = "192.168.88.133"
SPARK_MASTER_PORT = "7077"

# ==========================================
# 第1部分：Python(hdfs库) → Hadoop 集群 HDFS
# ==========================================
print("\n" + "=" * 60)
print(" 1. [Spark 集群 main] → Python(hdfs) → [Hadoop 集群 HDFS]")
print("=" * 60)

try:
    client = InsecureClient(
        f"http://{HADOOP_MASTER_IP}:{HADOOP_WEB_PORT}",
        user="hadoop"
    )
    contents = client.list("/user/test/")
    print(f"  Hadoop HDFS /user/test/ 目录: {contents}")

    with client.read("/user/test/students.csv") as reader:
        lines = sum(1 for _ in reader)
    print(f"  students.csv: {lines} 行")
    print("  [PASS] Spark集群main → Python → Hadoop集群HDFS 链路正常")

except Exception as e:
    print(f"  [FAIL] 无法连接 Hadoop 集群: {e}")
    print(f"  检查: Hadoop集群master的{HADOOP_WEB_PORT}端口是否可达？")
    exit(1)

# ==========================================
# 第2部分：PySpark → Hadoop 集群 HDFS
# ==========================================
print("\n" + "=" * 60)
print(" 2. [Spark 集群] PySpark → [Hadoop 集群] HDFS")
print("=" * 60)

try:
    spark = SparkSession.builder \
        .appName("Day3_CrossCluster") \
        .master(f"spark://{SPARK_MASTER_IP}:{SPARK_MASTER_PORT}") \
        .getOrCreate()

    hdfs_path = f"hdfs://{HADOOP_MASTER_IP}:{HADOOP_RPC_PORT}/user/test/students.csv"
    df = spark.read.csv(hdfs_path, header=True, inferSchema=True)
    cnt = df.count()

    if cnt == 8:
        print(f"  [PASS] PySpark 跨集群读取 Hadoop HDFS: {cnt} 行")
    else:
        print(f"  [FAIL] 预期 8 行，实际 {cnt} 行")

    print("\n  数据预览:")
    df.show(truncate=False)

    print("  按班级统计:")
    df.groupBy("班级").count().show()

    spark.stop()

except Exception as e:
    print(f"  [FAIL] PySpark 读 Hadoop HDFS 失败: {e}")
    print(f"  检查:")
    print(f"    1. Hadoop 集群 NameNode 的 {HADOOP_RPC_PORT} 端口是否可达？")
    print(f"    2. Spark 集群 slave 是否能访问 Hadoop 集群 master？")
    exit(1)

# ==========================================
# 总结
# ==========================================
print("\n" + "=" * 60)
print(" [PASS] 跨集群全链路验证通过")
print(" Spark 集群(main+slave) ←→ Hadoop 集群(master+slave1+slave2)")
print("=" * 60)
