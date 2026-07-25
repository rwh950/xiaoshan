#!/usr/bin/env python3

from pathlib import Path
from posixpath import dirname
import sys

from hdfs import InsecureClient


# ============================================================
# 1. 项目和HDFS配置
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parent.parent

NAMENODE_URL = "http://192.168.88.130:9870"
HDFS_USER = "hadoop"
HDFS_BASE = "/user/hadoop/ecommerce"


# ============================================================
# 2. 上传任务
# ============================================================

UPLOAD_TASKS = [
    {
        "name": "完整原始CSV",
        "local": (
            PROJECT_DIR
            / "data"
            / "raw"
            / "online_retail.csv"
        ),
        "hdfs": (
            f"{HDFS_BASE}"
            "/raw"
            "/online_retail.csv"
        ),
    },
    {
        "name": "1000行样例CSV",
        "local": (
            PROJECT_DIR
            / "data"
            / "sample"
            / "online_retail_sample.csv"
        ),
        "hdfs": (
            f"{HDFS_BASE}"
            "/sample"
            "/online_retail_sample.csv"
        ),
    },
    {
        "name": "数据质量报告",
        "local": (
            PROJECT_DIR
            / "docs"
            / "data_profile.md"
        ),
        "hdfs": (
            f"{HDFS_BASE}"
            "/reports"
            "/data_profile.md"
        ),
    },
]


def format_size(size_bytes: int) -> str:
    """把字节数转换成便于阅读的格式。"""

    size = float(size_bytes)

    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024 or unit == "GB":
            return f"{size:.2f} {unit}"

        size /= 1024

    return f"{size_bytes} B"


def check_local_files() -> None:
    """上传前检查所有本地文件是否存在。"""

    print("\n[1/4] 检查本地文件")

    missing_files = []

    for task in UPLOAD_TASKS:
        local_file = task["local"]

        if not local_file.exists():
            missing_files.append(local_file)
            print(f"缺失：{local_file}")
            continue

        if not local_file.is_file():
            missing_files.append(local_file)
            print(f"不是普通文件：{local_file}")
            continue

        local_size = local_file.stat().st_size

        print(
            f"正常：{task['name']}\n"
            f"  路径：{local_file}\n"
            f"  大小：{format_size(local_size)}"
        )

    if missing_files:
        print(
            "\n错误：存在缺失文件，停止上传。",
            file=sys.stderr,
        )
        sys.exit(1)

    print("本地文件检查通过")


def connect_hdfs() -> InsecureClient:
    """创建WebHDFS客户端并测试连接。"""

    print("\n[2/4] 连接HDFS")
    print(f"NameNode地址：{NAMENODE_URL}")
    print(f"HDFS用户：{HDFS_USER}")

    client = InsecureClient(
        NAMENODE_URL,
        user=HDFS_USER,
        timeout=120,
    )

    try:
        root_status = client.status(
            "/",
            strict=True,
        )
    except Exception as error:
        print(
            f"错误：无法连接WebHDFS：{error}",
            file=sys.stderr,
        )
        print(
            "请检查9870端口、WebHDFS配置和网络。",
            file=sys.stderr,
        )
        sys.exit(1)

    print(
        f"HDFS连接成功，根目录类型："
        f"{root_status.get('type')}"
    )

    return client


def upload_files(client: InsecureClient) -> None:
    """逐个上传文件，并比较本地和HDFS文件大小。"""

    print("\n[3/4] 上传文件")

    for index, task in enumerate(
        UPLOAD_TASKS,
        start=1,
    ):
        local_file = task["local"]
        hdfs_file = task["hdfs"]
        hdfs_directory = dirname(hdfs_file)

        print("\n" + "-" * 70)
        print(
            f"[{index}/{len(UPLOAD_TASKS)}] "
            f"{task['name']}"
        )
        print(f"本地：{local_file}")
        print(f"HDFS：{hdfs_file}")

        try:
            client.makedirs(hdfs_directory)

            uploaded_path = client.upload(
                hdfs_file,
                str(local_file),
                overwrite=True,
            )

            status = client.status(
                hdfs_file,
                strict=True,
            )
        except Exception as error:
            print(
                f"错误：上传失败：{error}",
                file=sys.stderr,
            )
            sys.exit(1)

        local_size = local_file.stat().st_size
        hdfs_size = int(status.get("length", -1))

        print(f"客户端返回路径：{uploaded_path}")
        print(f"本地文件大小：{format_size(local_size)}")
        print(f"HDFS文件大小：{format_size(hdfs_size)}")

        if local_size != hdfs_size:
            print(
                "错误：本地与HDFS文件大小不一致。",
                file=sys.stderr,
            )
            sys.exit(1)

        print("大小校验通过，上传成功")

    print("\n全部文件上传完成")


def list_hdfs_results(
    client: InsecureClient,
) -> None:
    """列出上传后的HDFS目录。"""

    print("\n[4/4] 查看HDFS目录")

    directories = [
        f"{HDFS_BASE}/raw",
        f"{HDFS_BASE}/sample",
        f"{HDFS_BASE}/reports",
    ]

    for directory in directories:
        try:
            entries = client.list(
                directory,
                status=True,
            )
        except Exception as error:
            print(
                f"错误：无法读取目录 {directory}："
                f"{error}",
                file=sys.stderr,
            )
            sys.exit(1)

        print(f"\n{directory}")

        for filename, status in entries:
            print(
                f"  {filename}"
                f"  {format_size(int(status['length']))}"
                f"  {status['type']}"
            )


def main() -> None:
    """执行完整上传流程。"""

    print("=" * 70)
    print("第三周 Day 4：WebHDFS上传")
    print("=" * 70)
    print(f"项目目录：{PROJECT_DIR}")
    print(f"HDFS目标目录：{HDFS_BASE}")

    check_local_files()
    client = connect_hdfs()
    upload_files(client)
    list_hdfs_results(client)

    print("\n" + "=" * 70)
    print("Day 4 上传任务执行成功")
    print("=" * 70)


if __name__ == "__main__":
    main()
