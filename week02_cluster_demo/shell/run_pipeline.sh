#!/usr/bin/env bash

set -Eeuo pipefail

# ============================================================
# 第二周一键数据流水线
#
# 用法：
# bash shell/run_pipeline.sh data/orders.csv 2026-07-18
#
# 流程：
# 1. 检查基础环境
# 2. 上传本地CSV到HDFS
# 3. 聚合HDFS订单数据
# 4. 写入HBase
# 5. 执行最终环境和结果检查
# ============================================================


# 获取项目根目录。
ROOT_DIR="$(
    cd "$(dirname "${BASH_SOURCE[0]}")/.."
    pwd
)"

cd "$ROOT_DIR"


usage() {
    cat <<'EOF'
用法：
  bash shell/run_pipeline.sh [本地CSV文件] [业务日期]

示例：
  bash shell/run_pipeline.sh data/orders.csv 2026-07-18

默认值：
  本地CSV：data/orders.csv
  业务日期：当前日期

日期格式：
  YYYY-MM-DD
EOF
}


# 显示帮助。
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi


# 命令行参数。
INPUT_ARGUMENT="${1:-data/orders.csv}"
BIZ_DATE="${2:-$(date '+%F')}"


# 相对路径统一转换成项目内绝对路径。
if [[ "$INPUT_ARGUMENT" = /* ]]; then
    INPUT_FILE="$INPUT_ARGUMENT"
else
    INPUT_FILE="$ROOT_DIR/$INPUT_ARGUMENT"
fi


CONFIG_FILE="$ROOT_DIR/config.env"
PYTHON="$ROOT_DIR/.venv/bin/python"
CHECK_SCRIPT="$ROOT_DIR/shell/check_environment.sh"

HDFS_PROGRAM="$ROOT_DIR/src/hdfs_demo.py"
AGGREGATE_PROGRAM="$ROOT_DIR/src/aggregate_orders.py"
HBASE_PROGRAM="$ROOT_DIR/src/hbase_writer.py"

LOG_DIR="$ROOT_DIR/logs"
OUTPUT_DIR="$ROOT_DIR/output"

mkdir -p "$LOG_DIR" "$OUTPUT_DIR"


# 校验日期格式。
if [[ ! "$BIZ_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "[错误] 日期格式必须是 YYYY-MM-DD" >&2
    echo "示例：2026-07-18" >&2
    exit 2
fi


# 校验日期是否真实存在，例如禁止2026-02-30。
if ! date -d "$BIZ_DATE" '+%F' >/dev/null 2>&1; then
    echo "[错误] 无效日期：$BIZ_DATE" >&2
    exit 2
fi


DATE_KEY="${BIZ_DATE//-/}"
RUN_TIME="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="$LOG_DIR/pipeline_${DATE_KEY}_${RUN_TIME}.log"

SUMMARY_FILE="$OUTPUT_DIR/user_summary_${DATE_KEY}.csv"


# 从这里开始，屏幕输出同时写入日志。
exec > >(tee -a "$LOG_FILE") 2>&1


on_error() {
    local exit_code=$?
    local line_number="${1:-未知}"
    local failed_command="${2:-未知}"

    set +e

    echo
    echo "========================================"
    echo "[流水线失败]"
    echo "退出码：$exit_code"
    echo "失败行：$line_number"
    echo "失败命令：$failed_command"
    echo "日志文件：$LOG_FILE"
    echo "========================================"

    exit "$exit_code"
}


trap 'on_error "$LINENO" "$BASH_COMMAND"' ERR


print_stage() {
    local stage_number="$1"
    local stage_name="$2"

    echo
    echo "========================================"
    echo "[$stage_number] $stage_name"
    echo "========================================"
}


check_file() {
    local file_path="$1"

    if [[ ! -f "$file_path" ]]; then
        echo "[错误] 文件不存在：$file_path" >&2
        return 1
    fi

    echo "[通过] 文件存在：$file_path"
}


check_port() {
    local host="$1"
    local port="$2"

    timeout 3 bash -c \
        "</dev/tcp/${host}/${port}" \
        >/dev/null 2>&1
}


echo "========================================"
echo "第二周 Python + HDFS + HBase 流水线"
echo "========================================"
echo "运行节点：$(hostname)"
echo "运行用户：$(whoami)"
echo "运行时间：$(date '+%F %T')"
echo "项目目录：$ROOT_DIR"
echo "输入文件：$INPUT_FILE"
echo "业务日期：$BIZ_DATE"
echo "日期目录：$DATE_KEY"
echo "日志文件：$LOG_FILE"


# ============================================================
# 第1阶段：基础检查
# ============================================================

print_stage "1/6" "检查基础环境"

check_file "$CONFIG_FILE"
check_file "$INPUT_FILE"
check_file "$HDFS_PROGRAM"
check_file "$AGGREGATE_PROGRAM"
check_file "$HBASE_PROGRAM"
check_file "$CHECK_SCRIPT"

if [[ ! -x "$PYTHON" ]]; then
    echo "[错误] 虚拟环境Python不存在：$PYTHON" >&2
    exit 3
fi

echo "[通过] Python虚拟环境：$PYTHON"
"$PYTHON" --version


# 加载项目配置并导出变量。
set -a
source "$CONFIG_FILE"
set +a


# 检查必需配置项。
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

for variable_name in "${REQUIRED_VARIABLES[@]}"; do
    variable_value="${!variable_name:-}"

    if [[ -z "$variable_value" ]]; then
        echo "[错误] 配置项为空：$variable_name" >&2
        exit 4
    fi

    echo "[通过] $variable_name=$variable_value"
done


# 检查Python依赖。
"$PYTHON" - <<'PY'
from hdfs import InsecureClient
import happybase
from thrift.transport.TTransport import TTransportException

print("[通过] hdfs模块正常")
print("[通过] happybase模块正常")
print("[通过] thrift模块正常")
PY


# 检查当前运行节点。
if [[ "$(hostname)" == "main" ]]; then
    echo "[通过] 当前位于Spark主节点main"
else
    echo "[警告] 当前主机不是main，当前主机：$(hostname)"
fi


# 检查NameNode WebHDFS。
WEBHDFS_TEST_URL="${HDFS_URL%/}/webhdfs/v1/?op=GETHOMEDIRECTORY&user.name=${HDFS_USER}"

curl -fsS "$WEBHDFS_TEST_URL" >/dev/null

echo "[通过] WebHDFS接口正常：$HDFS_URL"


# 检查DataNode端口。
for worker in $HADOOP_WORKERS; do
    if check_port "$worker" "$DATANODE_WEB_PORT"; then
        echo "[通过] DataNode端口正常：${worker}:${DATANODE_WEB_PORT}"
    else
        echo "[错误] DataNode端口无法连接：${worker}:${DATANODE_WEB_PORT}" >&2
        exit 5
    fi
done


# 检查HBase Thrift端口。
if check_port "$HBASE_HOST" "$HBASE_PORT"; then
    echo "[通过] HBase Thrift正常：${HBASE_HOST}:${HBASE_PORT}"
else
    echo "[错误] HBase Thrift无法连接：${HBASE_HOST}:${HBASE_PORT}" >&2
    exit 6
fi


# ============================================================
# 第2阶段：上传原始订单
# ============================================================

print_stage "2/6" "上传本地CSV到HDFS"

"$PYTHON" "$HDFS_PROGRAM" \
    --local-file "$INPUT_FILE" \
    --biz-date "$BIZ_DATE"

echo "[完成] 原始订单已上传到HDFS"


# ============================================================
# 第3阶段：聚合订单
# ============================================================

print_stage "3/6" "读取HDFS订单并聚合"

"$PYTHON" "$AGGREGATE_PROGRAM" \
    --biz-date "$BIZ_DATE"

if [[ ! -f "$SUMMARY_FILE" ]]; then
    echo "[错误] 聚合完成后未找到本地结果：$SUMMARY_FILE" >&2
    exit 7
fi

echo "[完成] 聚合结果：$SUMMARY_FILE"


# ============================================================
# 第4阶段：写入HBase
# ============================================================

print_stage "4/6" "将聚合结果写入HBase"

"$PYTHON" "$HBASE_PROGRAM" \
    --summary-file "$SUMMARY_FILE" \
    --biz-date "$BIZ_DATE"

echo "[完成] 聚合结果已写入HBase表：$HBASE_TABLE"


# ============================================================
# 第5阶段：最终验证
# ============================================================

print_stage "5/6" "执行最终环境和结果检查"

bash "$CHECK_SCRIPT" "$BIZ_DATE"

echo "[完成] 最终检查通过"


# ============================================================
# 第6阶段：输出结果
# ============================================================

print_stage "6/6" "输出流水线结果"

echo "本地聚合结果："
echo "----------------------------------------"
cat "$SUMMARY_FILE"
echo "----------------------------------------"

echo
echo "HDFS原始数据："
echo "${HDFS_BASE%/}/raw/${DATE_KEY}/orders.csv"

echo
echo "HDFS聚合结果："
echo "${HDFS_BASE%/}/result/${DATE_KEY}/user_summary.csv"

echo
echo "HBase表："
echo "$HBASE_TABLE"

echo
echo "HBase RowKey前缀："
echo "${DATE_KEY}#"

echo
echo "========================================"
echo "[全部成功] 第二周数据流水线执行完成"
echo "业务日期：$BIZ_DATE"
echo "本地结果：$SUMMARY_FILE"
echo "日志文件：$LOG_FILE"
echo "========================================"

exit 0
