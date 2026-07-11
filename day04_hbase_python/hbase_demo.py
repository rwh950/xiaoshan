import happybase

HBASE_HOST = "localhost"
HBASE_PORT = 9090

TABLE_NAME = "test_table"
COLUMN_FAMILY = "info"


def decode_bytes(value):
    """将HBase返回的bytes转换为可读字符串。"""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def decode_row(row):
    """解码一行数据中的列名和值。"""
    return {
        decode_bytes(column): decode_bytes(value)
        for column, value in row.items()
    }


connection = happybase.Connection(
    host=HBASE_HOST,
    port=HBASE_PORT,
    timeout=5000,
    autoconnect=False,
    transport="buffered",
    protocol="binary",
)

try:
    # 1. 建立连接
    connection.open()
    print("1. HBase连接成功")

    # 2. 获取现有表
    existing_tables = {
        decode_bytes(table_name)
        for table_name in connection.tables()
    }

    print("当前表：", sorted(existing_tables))

    # 3. 如果表不存在则创建
    if TABLE_NAME not in existing_tables:
        connection.create_table(
            TABLE_NAME,
            {
                COLUMN_FAMILY: {
                    "max_versions": 1
                }
            }
        )
        print(f"2. 创建表成功：{TABLE_NAME}")
    else:
        print(f"2. 表已存在，跳过建表：{TABLE_NAME}")

    # 4. 获取表对象
    table = connection.table(TABLE_NAME)

    # 5. 写入第一行
    table.put(
        b"row1",
        {
            b"info:name": "张三".encode("utf-8"),
            b"info:age": b"20",
            b"info:city": "杭州".encode("utf-8"),
        }
    )

    # 6. 写入第二行
    table.put(
        b"row2",
        {
            b"info:name": "李四".encode("utf-8"),
            b"info:age": b"21",
            b"info:city": "上海".encode("utf-8"),
        }
    )

    print("3. 写入数据成功")

    # 7. 按RowKey读取
    row = table.row(b"row1")
    print("4. 查询row1：")
    print(decode_row(row))

    # 8. 扫描全表
    print("5. 扫描全表：")

    for row_key, data in table.scan():
        print(
            "RowKey：",
            decode_bytes(row_key),
            "数据：",
            decode_row(data)
        )

except Exception as exc:
    print("程序执行失败：")
    print(type(exc).__name__, exc)
    raise

finally:
    connection.close()
    print("6. HBase连接已关闭")
