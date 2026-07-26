#!/usr/bin/env bash

# master节点上的HDFS命令绝对路径
REMOTE_HDFS_BIN="/usr/local/hadoop/bin/hdfs"

# -E：ERR错误陷阱可以进入函数和子Shell
# -e：任意关键命令失败后立即停止
# -u：使用未定义变量时立即报错
# pipefail：管道中任意命令失败，整个管道判定失败
set -Eeuo pipefail


# ============================================================
# 1. 项目配置
# ============================================================

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

VENV_DIR="${PROJECT_DIR}/.venv"
PYTHON_BIN="${VENV_DIR}/bin/python3"

LOG_DIR="${PROJECT_DIR}/logs"
RUN_TIME="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="${LOG_DIR}/week03_${RUN_TIME}.log"
LATEST_LOG="${LOG_DIR}/week03_latest.log"

HADOOP_HOST="192.168.88.130"
HADOOP_USER="hadoop"

SPARK_MASTER_URL="spark://main:7077"
SPARK_DRIVER_HOST="192.168.88.133"

HDFS_BASE="/user/hadoop/ecommerce"
HDFS_RELATIVE_PATH="${HDFS_BASE}/raw/online_retail.csv"

SOURCE_FILE="${PROJECT_DIR}/data/source/Online Retail.xlsx"

PREPARE_SCRIPT="${PROJECT_DIR}/scripts/prepare_raw.py"
INSPECT_SCRIPT="${PROJECT_DIR}/scripts/inspect_raw.py"
UPLOAD_SCRIPT="${PROJECT_DIR}/scripts/upload_hdfs.py"
SPARK_SCRIPT="${PROJECT_DIR}/scripts/spark_read_check.py"


# ============================================================
# 2. 日志配置
# ============================================================

mkdir -p "${LOG_DIR}"

# 所有正常输出和错误输出：
# 1. 显示在终端
# 2. 同时写入日志文件
exec > >(tee -a "${LOG_FILE}") 2>&1

# 建立一个固定名称的最新日志软链接
ln -sfn "$(basename "${LOG_FILE}")" "${LATEST_LOG}"


# ============================================================
# 3. 通用函数
# ============================================================

print_line() {
    printf '%*s\n' 72 '' | tr ' ' '='
}


print_step() {
    local step_number="$1"
    local step_name="$2"

    echo
    print_line
    echo "[${step_number}/4] ${step_name}"
    echo "开始时间：$(date '+%Y-%m-%d %H:%M:%S')"
    print_line
}


require_command() {
    local command_name="$1"

    if ! command -v "${command_name}" >/dev/null 2>&1; then
        echo "错误：找不到命令：${command_name}" >&2
        exit 1
    fi
}


require_file() {
    local file_path="$1"

    if [[ ! -f "${file_path}" ]]; then
        echo "错误：文件不存在：${file_path}" >&2
        exit 1
    fi
}


on_error() {
    local exit_code=$?
    local line_number="${BASH_LINENO[0]:-未知}"
    local failed_command="${BASH_COMMAND:-未知}"

    echo
    print_line
    echo "第三周流水线执行失败"
    echo "退出码：${exit_code}"
    echo "失败行号：${line_number}"
    echo "失败命令：${failed_command}"
    echo "日志文件：${LOG_FILE}"
    echo "失败时间：$(date '+%Y-%m-%d %H:%M:%S')"
    print_line

    exit "${exit_code}"
}


trap on_error ERR


# ============================================================
# 4. 前置检查
# ============================================================

cd "${PROJECT_DIR}"

print_line
echo "第三周电商数据采集流水线"
print_line
echo "当前主机：$(hostname)"
echo "当前用户：$(whoami)"
echo "项目目录：${PROJECT_DIR}"
echo "启动时间：$(date '+%Y-%m-%d %H:%M:%S')"
echo "本次日志：${LOG_FILE}"
echo "HDFS目录：${HDFS_BASE}"
echo "Spark Master：${SPARK_MASTER_URL}"
print_line


# 必须在main上执行
if [[ "$(hostname)" != "main" ]]; then
    echo "错误：本脚本必须在Spark主节点main上执行。" >&2
    echo "当前主机：$(hostname)" >&2
    exit 1
fi


# 必须是oyanx用户
if [[ "$(whoami)" != "oyanx" ]]; then
    echo "错误：本脚本应使用oyanx用户执行。" >&2
    echo "当前用户：$(whoami)" >&2
    exit 1
fi


# 检查关键命令
require_command ssh
require_command spark-submit
require_command tee
require_command date


# 检查虚拟环境
if [[ ! -d "${VENV_DIR}" ]]; then
    echo "错误：虚拟环境不存在：${VENV_DIR}" >&2
    echo "请先创建虚拟环境：" >&2
    echo "python3 -m venv .venv" >&2
    exit 1
fi


if [[ ! -x "${PYTHON_BIN}" ]]; then
    echo "错误：虚拟环境Python不可执行：${PYTHON_BIN}" >&2
    exit 1
fi


# 激活虚拟环境
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

echo
echo "Python环境："
echo "  Python路径：$(command -v python3)"
echo "  Python版本：$(python3 --version 2>&1)"
echo "  Spark命令：$(command -v spark-submit)"


# 检查Python依赖
python3 -c \
  "import pandas, openpyxl, hdfs; print('  Python依赖：正常')"


# 检查输入文件和脚本
require_file "${SOURCE_FILE}"
require_file "${PREPARE_SCRIPT}"
require_file "${INSPECT_SCRIPT}"
require_file "${UPLOAD_SCRIPT}"
require_file "${SPARK_SCRIPT}"


# 统一检查Python语法
echo
echo "检查Python脚本语法……"

python3 -m py_compile \
  "${PREPARE_SCRIPT}" \
  "${INSPECT_SCRIPT}" \
  "${UPLOAD_SCRIPT}" \
  "${SPARK_SCRIPT}"

echo "Python脚本语法检查通过"


# 检查能否连接Hadoop主节点
echo
echo "检查Hadoop主节点SSH连接……"

ssh \
  -o ConnectTimeout=8 \
  "${HADOOP_USER}@${HADOOP_HOST}" \
  "hostname && /usr/local/hadoop/bin/hdfs getconf -confKey fs.defaultFS" \
  >/dev/null

echo "Hadoop主节点SSH连接正常"


# ============================================================
# 5. 第一步：Excel转CSV
# ============================================================

print_step "1" "将原始Excel转换为标准CSV"

python3 "${PREPARE_SCRIPT}"

RAW_FILE="${PROJECT_DIR}/data/raw/online_retail.csv"
SAMPLE_FILE="${PROJECT_DIR}/data/sample/online_retail_sample.csv"

require_file "${RAW_FILE}"
require_file "${SAMPLE_FILE}"

echo
echo "步骤1完成："
echo "  完整CSV：${RAW_FILE}"
echo "  样例CSV：${SAMPLE_FILE}"


# ============================================================
# 6. 第二步：数据质量检查
# ============================================================

print_step "2" "检查原始数据质量"

python3 "${INSPECT_SCRIPT}"

PROFILE_FILE="${PROJECT_DIR}/docs/data_profile.md"

require_file "${PROFILE_FILE}"

echo
echo "步骤2完成："
echo "  数据质量报告：${PROFILE_FILE}"


# ============================================================
# 7. 第三步：上传HDFS
# ============================================================

print_step "3" "通过WebHDFS上传数据"

python3 "${UPLOAD_SCRIPT}"


# 上传后，使用Hadoop标准命令再次验证文件是否存在
echo
echo "使用Hadoop命令验证HDFS文件……"

ssh \
  -o ConnectTimeout=8 \
  "${HADOOP_USER}@${HADOOP_HOST}" \
  "${REMOTE_HDFS_BIN} dfs -test -e '${HDFS_RELATIVE_PATH}'"

echo "HDFS文件存在：${HDFS_RELATIVE_PATH}"

ssh \
  -o ConnectTimeout=8 \
  "${HADOOP_USER}@${HADOOP_HOST}" \
  "${REMOTE_HDFS_BIN} dfs -ls -h '${HDFS_RELATIVE_PATH}'"


# ============================================================
# 8. 第四步：Spark读取验证
# ============================================================

print_step "4" "Spark Standalone读取HDFS数据"


# 获取真实fs.defaultFS
HDFS_URI="$(
    ssh \
      -o ConnectTimeout=8 \
      "${HADOOP_USER}@${HADOOP_HOST}" \
      "${REMOTE_HDFS_BIN} getconf -confKey fs.defaultFS" \
    | tail -n 1 \
    | tr -d '\r\n' \
    | xargs
)"


# 防止出现之前的“HDFS输入路径不能为空”
if [[ -z "${HDFS_URI}" ]]; then
    echo "错误：没有获取到fs.defaultFS，HDFS_URI为空。" >&2
    exit 1
fi


# 检查URI格式
if [[ ! "${HDFS_URI}" =~ ^hdfs:// ]]; then
    echo "错误：fs.defaultFS格式异常：${HDFS_URI}" >&2
    echo "预期格式：hdfs://主机名:端口" >&2
    exit 1
fi


# 构造完整Spark输入路径
INPUT_PATH="${HDFS_URI%/}${HDFS_RELATIVE_PATH}"


# 再次防止输入路径为空
if [[ -z "${INPUT_PATH}" ]]; then
    echo "错误：Spark输入路径为空。" >&2
    exit 1
fi


echo "HDFS_URI：${HDFS_URI}"
echo "HDFS相对路径：${HDFS_RELATIVE_PATH}"
echo "Spark完整输入路径：${INPUT_PATH}"


# Spark Worker使用系统python3
export PYSPARK_PYTHON=python3


spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --deploy-mode client \
  --conf "spark.driver.host=${SPARK_DRIVER_HOST}" \
  --conf "spark.driver.bindAddress=0.0.0.0" \
  --conf "spark.pyspark.python=python3" \
  "${SPARK_SCRIPT}" \
  "${INPUT_PATH}"


# ============================================================
# 9. 完成
# ============================================================

echo
print_line
echo "第三周数据采集流水线执行成功"
echo "结束时间：$(date '+%Y-%m-%d %H:%M:%S')"
echo "完整日志：${LOG_FILE}"
echo "最新日志：${LATEST_LOG}"
echo "HDFS输入：${INPUT_PATH}"
print_line
