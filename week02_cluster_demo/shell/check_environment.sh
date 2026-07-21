#!/usr/bin/env bash

set -Eeuo pipefail

# 获取项目根目录。
ROOT_DIR="$(
    cd "$(dirname "${BASH_SOURCE[0]}")/.."
    pwd
)"

CONFIG_FILE="$ROOT_DIR/config.env"
PYTHON="$ROOT_DIR/.venv/bin/python"

# 第一个参数是业务日期。
# 没有传参数时，默认检查2026-07-18。
BIZ_DATE="${1:-2026-07-18}"

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0


print_title() {
    echo
    echo "========================================"
    echo "$1"
    echo "========================================"
}


pass() {
    echo "[通过] $*"
    ((PASS_COUNT += 1))
}


fail() {
    echo "[失败] $*" >&2
    ((FAIL_COUNT += 1))
}


warn() {
    echo "[警告] $*"
    ((WARN_COUNT += 1))
}


check_port() {
    local host="$1"
    local port="$2"

    timeout 3 bash -c \
        "</dev/tcp/${host}/${port}" \
        >/dev/null 2>&1
}


check_required_file() {
    local file_path="$1"

    if [[ -f "$file_path" ]]; then
        pass "文件存在：$file_path"
    else
        fail "文件不存在：$file_path"
    fi
}


# ------------------------------
# 1. 检查业务日期
# ------------------------------

if [[ ! "$BIZ_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "[错误] 日期格式必须是 YYYY-MM-DD" >&2
    echo "示例：bash shell/check_environment.sh 2026-07-18" >&2
    exit 2
fi

DATE_KEY="${BIZ_DATE//-/}"


# ------------------------------
# 2. 检查配置文件
# ------------------------------

print_title "1. 检查项目配置"

echo "项目目录：$ROOT_DIR"
echo "业务日期：$BIZ_DATE"
echo "日期目录：$DATE_KEY"

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "[致命错误] 配置文件不存在：$CONFIG_FILE" >&2
    exit 3
fi

pass "配置文件存在：$CONFIG_FILE"

# 将config.env中的变量导出到当前Shell环境。
set -a
source "$CONFIG_FILE"
set +a

REQUIRED_VARIABLES=(
    HDFS_URL
    HDFS_USER
    HDFS_BASE
    DATANODE_WEB_PORT
    HADOOP_WORKERS
    HBASE_HOST
    HBASE_PORT
    HBASE_TABLE
    HBASE_FAMILY
)

MISSING_VARIABLES=0

for variable_name in "${REQUIRED_VARIABLES[@]}"; do
    variable_value="${!variable_name:-}"

    if [[ -n "$variable_value" ]]; then
        pass "配置项已设置：$variable_name=$variable_value"
    else
        fail "配置项为空：$variable_name"
        MISSING_VARIABLES=1
    fi
done

if ((MISSING_VARIABLES != 0)); then
    echo "[致命错误] config.env配置不完整，停止后续检查" >&2
    exit 4
fi


# ------------------------------
# 3. 检查运行节点
# ------------------------------

print_title "2. 检查当前运行节点"

CURRENT_HOST="$(hostname)"
CURRENT_IPS="$(hostname -I 2>/dev/null || true)"

echo "当前主机：$CURRENT_HOST"
echo "当前IP：$CURRENT_IPS"
echo "当前用户：$(whoami)"

if [[ "$CURRENT_HOST" == "main" ]]; then
    pass "当前正在Spark主节点main运行"
else
    warn "当前主机名不是main，实际主机名为：$CURRENT_HOST"
fi


# ------------------------------
# 4. 检查本地项目文件
# ------------------------------

print_title "3. 检查本地项目文件"

check_required_file "$ROOT_DIR/data/orders.csv"
check_required_file "$ROOT_DIR/src/hdfs_demo.py"
check_required_file "$ROOT_DIR/src/aggregate_orders.py"
check_required_file "$ROOT_DIR/src/hbase_writer.py"

SUMMARY_FILE="$ROOT_DIR/output/user_summary_${DATE_KEY}.csv"

check_required_file "$SUMMARY_FILE"


# ------------------------------
# 5. 检查Python虚拟环境
# ------------------------------

print_title "4. 检查Python虚拟环境"

if [[ -x "$PYTHON" ]]; then
    pass "虚拟环境Python存在：$PYTHON"
    "$PYTHON" --version
else
    fail "虚拟环境Python不存在：$PYTHON"
fi

if [[ -x "$PYTHON" ]]; then
    if "$PYTHON" - <<'PY'
from hdfs import InsecureClient
import happybase
from thrift.transport.TTransport import TTransportException

print("hdfs模块：正常")
print("happybase模块：正常")
print("thrift模块：正常")
PY
    then
        pass "Python依赖导入成功"
    else
        fail "Python依赖导入失败"
    fi
fi


# ------------------------------
# 6. 检查主机名解析
# ------------------------------

print_title "5. 检查主机名解析"

HOSTS_TO_CHECK=(
    master
    slave1
    slave2
)

for host in "${HOSTS_TO_CHECK[@]}"; do
    if getent ahostsv4 "$host" >/dev/null 2>&1; then
        resolved_ip="$(
            getent ahostsv4 "$host" |
            awk 'NR == 1 {print $1}'
        )"

        pass "$host 可以解析为 $resolved_ip"
    else
        fail "无法解析主机名：$host"
    fi
done

if getent ahostsv4 "$HBASE_HOST" >/dev/null 2>&1; then
    pass "HBase地址可以解析：$HBASE_HOST"
else
    fail "HBase地址无法解析：$HBASE_HOST"
fi


# ------------------------------
# 7. 检查NameNode WebHDFS
# ------------------------------

print_title "6. 检查NameNode WebHDFS"

HDFS_URL_CLEAN="${HDFS_URL%/}"
HDFS_ENDPOINT="${HDFS_URL_CLEAN#http://}"
HDFS_ENDPOINT="${HDFS_ENDPOINT#https://}"

NAMENODE_HOST="${HDFS_ENDPOINT%%:*}"
NAMENODE_PORT="${HDFS_ENDPOINT##*:}"

echo "NameNode主机：$NAMENODE_HOST"
echo "NameNode端口：$NAMENODE_PORT"

if check_port "$NAMENODE_HOST" "$NAMENODE_PORT"; then
    pass "NameNode端口正常：${NAMENODE_HOST}:${NAMENODE_PORT}"
else
    fail "NameNode端口无法连接：${NAMENODE_HOST}:${NAMENODE_PORT}"
fi

WEBHDFS_TEST_URL="${HDFS_URL_CLEAN}/webhdfs/v1/?op=GETHOMEDIRECTORY&user.name=${HDFS_USER}"

if curl -fsS "$WEBHDFS_TEST_URL" >/dev/null; then
    pass "WebHDFS REST接口访问成功"
else
    fail "WebHDFS REST接口访问失败：$WEBHDFS_TEST_URL"
fi


# ------------------------------
# 8. 检查两个DataNode端口
# ------------------------------

print_title "7. 检查DataNode Web端口"

for worker in $HADOOP_WORKERS; do
    if check_port "$worker" "$DATANODE_WEB_PORT"; then
        pass "${worker}:${DATANODE_WEB_PORT} 可以访问"
    else
        fail "${worker}:${DATANODE_WEB_PORT} 无法访问"
    fi
done


# ------------------------------
# 9. 检查HDFS文件
# ------------------------------

print_title "8. 检查HDFS数据文件"

RAW_HDFS_PATH="${HDFS_BASE%/}/raw/${DATE_KEY}/orders.csv"
RESULT_HDFS_PATH="${HDFS_BASE%/}/result/${DATE_KEY}/user_summary.csv"

export RAW_HDFS_PATH
export RESULT_HDFS_PATH

echo "HDFS原始文件：$RAW_HDFS_PATH"
echo "HDFS结果文件：$RESULT_HDFS_PATH"

if [[ -x "$PYTHON" ]]; then
    if "$PYTHON" - <<'PY'
import os
import sys

from hdfs import InsecureClient


client = InsecureClient(
    url=os.environ["HDFS_URL"],
    user=os.environ["HDFS_USER"],
    timeout=30,
)

paths = [
    ("原始订单", os.environ["RAW_HDFS_PATH"]),
    ("聚合结果", os.environ["RESULT_HDFS_PATH"]),
]

missing = False

for label, path in paths:
    status = client.status(path, strict=False)

    if status is None:
        print(f"[Python检查失败] {label}不存在：{path}")
        missing = True
    else:
        print(
            f"[Python检查成功] {label}存在：{path}，"
            f"大小={status['length']}字节，"
            f"所有者={status['owner']}，"
            f"副本={status['replication']}"
        )

if missing:
    sys.exit(1)
PY
    then
        pass "HDFS原始数据和聚合结果均存在"
    else
        fail "HDFS文件检查未通过"
    fi
fi


# ------------------------------
# 10. 检查HBase Thrift
# ------------------------------

print_title "9. 检查HBase Thrift"

if check_port "$HBASE_HOST" "$HBASE_PORT"; then
    pass "HBase Thrift端口正常：${HBASE_HOST}:${HBASE_PORT}"
else
    fail "HBase Thrift端口无法访问：${HBASE_HOST}:${HBASE_PORT}"
fi


# ------------------------------
# 11. 检查HBase表
# ------------------------------

print_title "10. 检查HBase表"

if [[ -x "$PYTHON" ]]; then
    if "$PYTHON" - <<'PY'
import os
import sys

import happybase


host = os.environ["HBASE_HOST"]
port = int(os.environ["HBASE_PORT"])
table_name = os.environ["HBASE_TABLE"]

connection = happybase.Connection(
    host=host,
    port=port,
    timeout=5000,
    autoconnect=False,
    transport="buffered",
    protocol="binary",
)

try:
    connection.open()

    tables = {
        item.decode("utf-8")
        if isinstance(item, bytes)
        else str(item)
        for item in connection.tables()
    }

    print("当前HBase表：")

    if tables:
        for item in sorted(tables):
            print(f"- {item}")
    else:
        print("- 当前没有表")

    if table_name not in tables:
        print(f"[Python检查失败] 表不存在：{table_name}")
        sys.exit(1)

    table = connection.table(table_name)

    row_count = 0

    for _row_key, _data in table.scan(
        limit=10,
    ):
        row_count += 1

    print(f"[Python检查成功] 表存在：{table_name}")
    print(f"[Python检查成功] 前10条扫描到：{row_count}条")

finally:
    connection.close()
PY
    then
        pass "HBase连接和表检查通过"
    else
        fail "HBase连接或表检查失败"
    fi
fi


# ------------------------------
# 12. 检查Spark Master
# ------------------------------

print_title "11. 检查本机Spark进程"

if command -v jps >/dev/null 2>&1; then
    if jps | awk '{print $2}' | grep -qx "Master"; then
        pass "Spark Master进程正在运行"
    else
        warn "没有检测到Spark Master进程，本阶段HDFS/HBase程序仍可运行"
    fi
else
    warn "系统中没有找到jps命令"
fi


# ------------------------------
# 13. 输出总结
# ------------------------------

print_title "环境检查总结"

echo "通过数量：$PASS_COUNT"
echo "失败数量：$FAIL_COUNT"
echo "警告数量：$WARN_COUNT"

if ((FAIL_COUNT > 0)); then
    echo
    echo "[结论] 环境检查未通过，请处理失败项目。"
    exit 1
fi

echo
echo "[结论] 所有关键环境检查通过。"
exit 0
