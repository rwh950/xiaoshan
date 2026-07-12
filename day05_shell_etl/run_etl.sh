#!/usr/bin/env bash

# Day 5：Shell调度Spark Standalone ETL
#
# 执行位置：Spark主节点main
# 数据输入：Hadoop完全分布式HDFS
# 数据输出：Hadoop完全分布式HDFS

set -Eeuo pipefail

SCRIPT_DIR="$(
    cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
    pwd
)"

LOG_DIR="${SCRIPT_DIR}/logs"
mkdir -p "$LOG_DIR"

LOG_FILE="${LOG_DIR}/etl_$(date '+%Y%m%d_%H%M%S').log"

SECONDS=0

log() {
    printf '[%s] %s\n' \
        "$(date '+%Y-%m-%d %H:%M:%S')" \
        "$*" \
        | tee -a "$LOG_FILE"
}

fail() {
    log "[FAIL] $*"
    exit 1
}

on_error() {
    local exit_code=$?
    local line_number=$1
    local command_text=$2

    log "[FAIL] 未处理错误"
    log "[FAIL] 行号：${line_number}"
    log "[FAIL] 命令：${command_text}"
    log "[FAIL] 退出码：${exit_code}"

    exit "$exit_code"
}

trap 'on_error "$LINENO" "$BASH_COMMAND"' ERR

# 可以通过环境变量覆盖：
#
# HDFS_BASE=/user/其他用户 ./run_etl.sh
# SPARK_MASTER=spark://192.168.88.133:7077 ./run_etl.sh
# HDFS_URI=hdfs://master:9000 ./run_etl.sh

HDFS_BASE="${HDFS_BASE:-/user/oyanx}"
HDFS_INPUT="${HDFS_INPUT:-${HDFS_BASE}/raw/sample_scores.csv}"
HDFS_OUTPUT="${HDFS_OUTPUT:-${HDFS_BASE}/clean/aggregated}"

SPARK_MASTER="${SPARK_MASTER:-spark://192.168.88.133:7077}"
HDFS_URI="${HDFS_URI:-}"

log "================================================"
log "Day 5 ETL开始"
log "脚本目录：${SCRIPT_DIR}"
log "日志文件：${LOG_FILE}"
log "================================================"

# ------------------------------------------------
# STEP 1：检查运行环境
# ------------------------------------------------

log "[STEP 1/6] 检查运行环境"

command -v hdfs >/dev/null 2>&1 \
    || fail "找不到Apache Hadoop的hdfs命令"

command -v spark-submit >/dev/null 2>&1 \
    || fail "找不到spark-submit命令"

if ! HDFS_VERSION="$(
    hdfs version 2>&1 | head -n 1
)"; then
    fail "hdfs命令无法正常执行"
fi

if [[ "$HDFS_VERSION" != Hadoop* ]]; then
    fail "当前hdfs不是Apache Hadoop命令：${HDFS_VERSION}"
fi

log "[OK] hdfs：$(command -v hdfs)"
log "[OK] ${HDFS_VERSION}"
log "[OK] spark-submit：$(command -v spark-submit)"

if [[ -z "$HDFS_URI" ]]; then
    HDFS_URI="$(
        hdfs getconf -confKey fs.defaultFS
    )"
fi

HDFS_URI="${HDFS_URI%/}"

if [[ "$HDFS_URI" != hdfs://* ]]; then
    fail "fs.defaultFS不是HDFS地址：${HDFS_URI}"
fi

INPUT_URI="${HDFS_URI}${HDFS_INPUT}"
OUTPUT_URI="${HDFS_URI}${HDFS_OUTPUT}"

log "[OK] HDFS URI：${HDFS_URI}"
log "[OK] Spark Master：${SPARK_MASTER}"

# ------------------------------------------------
# STEP 2：检查HDFS输入文件
# ------------------------------------------------

log "[STEP 2/6] 检查HDFS原始数据"

if ! hdfs dfs -test -e "$HDFS_INPUT"; then
    fail "输入文件不存在：${HDFS_INPUT}"
fi

FILE_SIZE="$(
    hdfs dfs -du -s "$HDFS_INPUT" |
        awk '{print $1}'
)"

log "[OK] 输入文件：${HDFS_INPUT}"
log "[OK] 输入大小：${FILE_SIZE}字节"

# ------------------------------------------------
# STEP 3：准备HDFS输出目录
# ------------------------------------------------

log "[STEP 3/6] 准备HDFS输出目录"

HDFS_OUTPUT_PARENT="$(dirname "$HDFS_OUTPUT")"

hdfs dfs -mkdir -p "$HDFS_OUTPUT_PARENT"

if hdfs dfs -test -e "$HDFS_OUTPUT"; then
    hdfs dfs -rm -r -skipTrash "$HDFS_OUTPUT"
    log "[OK] 已删除旧输出：${HDFS_OUTPUT}"
else
    log "[INFO] 没有旧输出，无需删除"
fi

# ------------------------------------------------
# STEP 4：提交Spark Standalone任务
# ------------------------------------------------

log "[STEP 4/6] 提交Spark任务"
log "[INFO] 输入URI：${INPUT_URI}"
log "[INFO] 输出URI：${OUTPUT_URI}"

if spark-submit \
    --master "$SPARK_MASTER" \
    --deploy-mode client \
    --name "Day5_ETL" \
    "${SCRIPT_DIR}/etl_aggregate.py" \
    "$INPUT_URI" \
    "$OUTPUT_URI" \
    2>&1 | tee -a "$LOG_FILE"
then
    log "[OK] Spark任务返回成功"
else
    SPARK_EXIT_CODE=$?
    log "[FAIL] Spark任务执行失败"
    log "[FAIL] Spark退出码：${SPARK_EXIT_CODE}"
    exit "$SPARK_EXIT_CODE"
fi

# ------------------------------------------------
# STEP 5：验证HDFS输出
# ------------------------------------------------

log "[STEP 5/6] 验证HDFS输出"

if ! hdfs dfs -test -e "${HDFS_OUTPUT}/_SUCCESS"; then
    fail "没有找到_SUCCESS文件"
fi

if ! PART_LIST="$(
    hdfs dfs -ls "${HDFS_OUTPUT}/part-*.csv" 2>/dev/null
)"; then
    fail "没有找到CSV结果文件"
fi

PART_FILE="$(
    printf '%s\n' "$PART_LIST" |
        awk 'NR == 1 {print $8}'
)"

if [[ -z "$PART_FILE" ]]; then
    fail "无法取得结果文件路径"
fi

log "[OK] 找到_SUCCESS"
log "[OK] 结果文件：${PART_FILE}"
log "[INFO] 聚合结果："

hdfs dfs -cat "$PART_FILE" |
    tee -a "$LOG_FILE"

# ------------------------------------------------
# STEP 6：统计输出
# ------------------------------------------------

log "[STEP 6/6] 统计输出数据"

ROW_COUNT="$(
    hdfs dfs -cat "$PART_FILE" |
        awk '
            NR > 1 && NF > 0 {
                count++
            }
            END {
                print count + 0
            }
        '
)"

AVG_SCORE="$(
    hdfs dfs -cat "$PART_FILE" |
        awk -F',' '
            NR > 1 && NF > 0 {
                sum += $4
                count++
            }
            END {
                if (count > 0) {
                    printf "%.1f", sum / count
                } else {
                    print "N/A"
                }
            }
        '
)"

log "[OK] 聚合学生数：${ROW_COUNT}"
log "[OK] 所有学生平均分的平均值：${AVG_SCORE}"

log "================================================"
log "ETL执行成功"
log "输入：${INPUT_URI}"
log "输出：${OUTPUT_URI}"
log "日志：${LOG_FILE}"
log "耗时：${SECONDS}秒"
log "================================================"

exit 0
