#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path

import requests
from hdfs import InsecureClient
from hdfs.util import HdfsError


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(
        description="通过WebHDFS完成上传、查看、读取和下载测试"
    )
    parser.add_argument(
        "--local-file",
        required=True,
        help="需要上传的本地文件，例如 data/orders.csv",
    )
    parser.add_argument(
        "--biz-date",
        required=True,
        help="业务日期，格式必须为 YYYY-MM-DD",
    )
    return parser.parse_args()


def validate_date(date_text: str) -> str:
    """检查日期格式，并转换为HDFS目录使用的YYYYMMDD。"""
    parts = date_text.split("-")

    if (
        len(parts) != 3
        or len(parts[0]) != 4
        or len(parts[1]) != 2
        or len(parts[2]) != 2
        or not all(part.isdigit() for part in parts)
    ):
        raise ValueError(
            f"业务日期格式错误：{date_text}，必须使用 YYYY-MM-DD"
        )

    return "".join(parts)


def get_required_env(name: str) -> str:
    """读取必需的环境变量。"""
    value = os.environ.get(name, "").strip()

    if not value:
        raise RuntimeError(f"缺少环境变量：{name}")

    return value


def create_client() -> InsecureClient:
    """创建WebHDFS客户端。"""
    hdfs_url = get_required_env("HDFS_URL")
    hdfs_user = get_required_env("HDFS_USER")

    return InsecureClient(
        url=hdfs_url,
        user=hdfs_user,
        timeout=30,
    )


def sha256_file(path: Path) -> str:
    """计算本地文件SHA-256，用于校验下载结果。"""
    digest = hashlib.sha256()

    with path.open("rb") as file:
        while True:
            block = file.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def print_directory(
    client: InsecureClient,
    remote_dir: str,
) -> None:
    """输出HDFS目录中的文件信息。"""
    print("\n[目录列表]")

    entries = client.list(remote_dir, status=True)

    if not entries:
        print("目录为空")
        return

    for name, status in entries:
        print(
            f"- 名称={name}, "
            f"类型={status.get('type')}, "
            f"大小={status.get('length')}字节, "
            f"所有者={status.get('owner')}, "
            f"权限={status.get('permission')}"
        )


def main() -> int:
    args = parse_args()

    local_file = Path(args.local_file).expanduser().resolve()

    if not local_file.exists():
        print(
            f"[错误] 本地文件不存在：{local_file}",
            file=sys.stderr,
        )
        return 1

    if not local_file.is_file():
        print(
            f"[错误] 指定路径不是普通文件：{local_file}",
            file=sys.stderr,
        )
        return 1

    try:
        date_key = validate_date(args.biz_date)

        hdfs_url = get_required_env("HDFS_URL")
        hdfs_user = get_required_env("HDFS_USER")
        hdfs_base = get_required_env("HDFS_BASE").rstrip("/")

        remote_dir = f"{hdfs_base}/raw/{date_key}"
        remote_file = f"{remote_dir}/{local_file.name}"

        output_dir = Path("output").resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        downloaded_file = (
            output_dir
            / f"downloaded_{date_key}_{local_file.name}"
        )

        print("========================================")
        print("Python WebHDFS连接测试")
        print("========================================")
        print(f"[配置] NameNode地址：{hdfs_url}")
        print(f"[配置] HDFS用户：{hdfs_user}")
        print(f"[本地] 上传文件：{local_file}")
        print(f"[HDFS] 目标目录：{remote_dir}")
        print(f"[HDFS] 目标文件：{remote_file}")

        client = create_client()

        print("\n[1/7] 检查HDFS连接")

        root_status = client.status("/", strict=True)

        print(
            "[成功] 已连接HDFS，"
            f"根目录所有者={root_status.get('owner')}, "
            f"权限={root_status.get('permission')}"
        )

        print("\n[2/7] 创建HDFS目录")

        client.makedirs(remote_dir, permission="755")

        print(f"[成功] 目录已准备：{remote_dir}")

        print("\n[3/7] 上传本地文件")

        uploaded_path = client.upload(
            hdfs_path=remote_file,
            local_path=str(local_file),
            overwrite=True,
            cleanup=True,
        )

        print(f"[成功] 上传结果：{uploaded_path}")

        print("\n[4/7] 查询HDFS文件状态")

        file_status = client.status(
            remote_file,
            strict=True,
        )

        print(f"文件类型：{file_status.get('type')}")
        print(f"文件长度：{file_status.get('length')} 字节")
        print(f"所有者：{file_status.get('owner')}")
        print(f"所属组：{file_status.get('group')}")
        print(f"权限：{file_status.get('permission')}")
        print(f"副本数：{file_status.get('replication')}")

        local_size = local_file.stat().st_size
        remote_size = int(file_status.get("length", -1))

        if local_size != remote_size:
            raise RuntimeError(
                "上传前后文件大小不一致："
                f"本地={local_size}，HDFS={remote_size}"
            )

        print(
            f"[成功] 文件大小一致：{local_size} 字节"
        )

        print_directory(client, remote_dir)

        print("\n[5/7] 在线读取HDFS文件")

        with client.read(
            remote_file,
            encoding="utf-8",
        ) as reader:
            content = reader.read()

        print("---------- HDFS文件内容 ----------")
        print(content.rstrip())
        print("----------------------------------")

        print("\n[6/7] 下载HDFS文件到main")

        if downloaded_file.exists():
            downloaded_file.unlink()

        downloaded_path = client.download(
            hdfs_path=remote_file,
            local_path=str(downloaded_file),
            overwrite=True,
        )

        print(f"[成功] 下载位置：{downloaded_path}")

        print("\n[7/7] 校验本地文件和下载文件")

        original_hash = sha256_file(local_file)
        downloaded_hash = sha256_file(downloaded_file)

        print(f"原始文件SHA-256：{original_hash}")
        print(f"下载文件SHA-256：{downloaded_hash}")

        if original_hash != downloaded_hash:
            raise RuntimeError(
                "校验失败：上传前文件和下载后文件内容不同"
            )

        print("\n========================================")
        print("[全部成功] Python已经完成HDFS上传、读取和下载")
        print(f"[HDFS文件] {remote_file}")
        print(f"[下载文件] {downloaded_file}")
        print("========================================")

        return 0

    except ValueError as exc:
        print(f"[参数错误] {exc}", file=sys.stderr)
        return 2

    except HdfsError as exc:
        print(f"[HDFS错误] {exc}", file=sys.stderr)
        return 3

    except requests.RequestException as exc:
        print(f"[网络错误] {exc}", file=sys.stderr)
        return 4

    except (OSError, RuntimeError) as exc:
        print(f"[运行错误] {exc}", file=sys.stderr)
        return 5


if __name__ == "__main__":
    raise SystemExit(main())
