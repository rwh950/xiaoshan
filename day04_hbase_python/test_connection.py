import happybase

HBASE_HOST = "192.168.88.151"
HBASE_PORT = 9090

connection = happybase.Connection(
    host=HBASE_HOST,
    port=HBASE_PORT,
    timeout=5000,
    autoconnect=False,
    transport="buffered",
    protocol="binary",
)

try:
    connection.open()
    print("HBase连接成功")
    print("现有表：", connection.tables())
finally:
    connection.close()
