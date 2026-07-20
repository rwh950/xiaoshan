#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from hdfs import InsecureClient
from hdfs.util import HdfsError


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    """读取命令行参数。"""
    parser = argparse.ArgumentParser(
        description="从HDFS读取订单，并按user_id统计订单数和消费总额"
    )
    parser.add_argument(
        "--biz-date",
        required=True,
        help="业务日期，格式为 YYYY-MM-DD，例如 2026-07-18",
    )
    return parser.parse_args()


def convert_date(date_text: str) -> str:
    """检查日期格式并转换为YYYYMMDD。"""
    try:
        date_value = datetime.strptime(date_text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"业务日期格式错误：{date_text}，必须使用 YYYY-MM-DD"
        ) from exc

    return date_value.strftime("%Y%m%d")


def get_required_env(name: str) -> str:
    """读取必需的环境变量。"""
    value = os.environ.get(name, "").strip()

    if not value:
        raise RuntimeError(
            f"缺少环境变量 {name}，请先执行 source config.env"
        )

    return value


def create_hdfs_client() -> InsecureClient:
    """创建WebHDFS客户端。"""
    return InsecureClient(
        url=get_required_env("HDFS_URL"),
        user=get_required_env("HDFS_USER"),
        timeout=30,
    )


def aggregate_orders(
    client: InsecureClient,
    input_path: str,
) -> tuple[dict[str, dict[str, object]], int, int]:
    """
    从HDFS读取CSV并聚合。

    返回：
    1. 用户聚合结果
    2. 有效数据行数
    3. 跳过的数据行数
    """
    statistics: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "order_count": 0,
            "total_amount": Decimal("0.00"),
        }
    )

    valid_rows = 0
    skipped_rows = 0

    with client.read(input_path, encoding="utf-8") as reader:
        csv_reader = csv.DictReader(reader)

        if csv_reader.fieldnames is None:
            raise ValueError("CSV文件没有表头")

        actual_fields = {
            field.strip()
            for field in csv_reader.fieldnames
            if field is not None
        }

        required_fields = {
            "order_id",
            "user_id",
            "amount",
        }

        missing_fields = required_fields - actual_fields

        if missing_fields:
            raise ValueError(
                "CSV缺少必要字段："
                + ", ".join(sorted(missing_fields))
            )

        for line_number, raw_row in enumerate(
            csv_reader,
            start=2,
        ):
            # 清理字段名和值两端的空格。
            row = {
                (key or "").strip(): (value or "").strip()
                for key, value in raw_row.items()
            }

            order_id = row.get("order_id", "")
            user_id = row.get("user_id", "")
            amount_text = row.get("amount", "")

            if not order_id:
                print(
                    f"[跳过] 第{line_number}行 order_id 为空",
                    file=sys.stderr,
                )
                skipped_rows += 1
                continue

            if not user_id:
                print(
                    f"[跳过] 第{line_number}行 user_id 为空",
                    file=sys.stderr,
                )
                skipped_rows += 1
                continue

            try:
                amount = Decimal(amount_text)
            except InvalidOperation:
                print(
                    f"[跳过] 第{line_number}行金额非法：{amount_text}",
                    file=sys.stderr,
                )
                skipped_rows += 1
                continue

            if amount < 0:
                print(
                    f"[跳过] 第{line_number}行金额为负数：{amount}",
                    file=sys.stderr,
                )
                skipped_rows += 1
                continue

            statistics[user_id]["order_count"] += 1
            statistics[user_id]["total_amount"] += amount
            valid_rows += 1

    return dict(statistics), valid_rows, skipped_rows


def build_output_csv(
    statistics: dict[str, dict[str, object]],
) -> str:
    """把聚合结果转换成CSV文本。"""
    buffer = io.StringIO(newline="")

    writer = csv.writer(
        buffer,
        lineterminator="\n",
    )

    writer.writerow(
        [
            "user_id",
            "order_count",
            "total_amount",
        ]
    )

    for user_id in sorted(statistics):
        order_count = int(
            statistics[user_id]["order_count"]
        )
        total_amount = Decimal(
            statistics[user_id]["total_amount"]
        )

        writer.writerow(
            [
                user_id,
                order_count,
                format(total_amount, ".2f"),
            ]
        )

    return buffer.getvalue()


def main() -> int:
    args = parse_args()

    try:
        date_key = convert_date(args.biz_date)

        hdfs_base = get_required_env("HDFS_BASE").rstrip("/")

        input_path = (
            f"{hdfs_base}/raw/{date_key}/orders.csv"
        )

        hdfs_output_dir = (
            f"{hdfs_base}/result/{date_key}"
        )

        hdfs_output_path = (
            f"{hdfs_output_dir}/user_summary.csv"
        )

        local_output_dir = PROJECT_ROOT / "output"
        local_output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        local_output_path = (
            local_output_dir
            / f"user_summary_{date_key}.csv"
        )

        print("========================================")
        print("HDFS订单聚合程序")
        print("========================================")
        print(f"[业务日期] {args.biz_date}")
        print(f"[HDFS输入] {input_path}")
        print(f"[HDFS输出] {hdfs_output_path}")
        print(f"[本地输出] {local_output_path}")

        client = create_hdfs_client()

        print("\n[1/6] 检查HDFS输入文件")

        input_status = client.status(
            input_path,
            strict=False,
        )

        if input_status is None:
            print(
                f"[错误] HDFS输入文件不存在：{input_path}",
                file=sys.stderr,
            )
            return 1

        print(
            "[成功] 输入文件存在，"
            f"大小={input_status['length']}字节，"
            f"副本数={input_status['replication']}"
        )

        print("\n[2/6] 读取并聚合订单")

        statistics, valid_rows, skipped_rows = (
            aggregate_orders(
                client=client,
                input_path=input_path,
            )
        )

        if not statistics:
            print(
                "[错误] 没有可用于聚合的订单数据",
                file=sys.stderr,
            )
            return 2

        print(f"[成功] 有效订单数：{valid_rows}")
        print(f"[成功] 跳过订单数：{skipped_rows}")
        print(f"[成功] 用户数量：{len(statistics)}")

        print("\n[3/6] 生成聚合结果")

        output_content = build_output_csv(statistics)

        print("---------- 聚合结果 ----------")
        print(output_content.rstrip())
        print("------------------------------")

        print("\n[4/6] 保存本地结果")

        local_output_path.write_text(
            output_content,
            encoding="utf-8",
        )

        print(f"[成功] 本地文件：{local_output_path}")

        print("\n[5/6] 写入HDFS结果目录")

        client.makedirs(
            hdfs_output_dir,
            permission="755",
        )

        client.write(
            hdfs_path=hdfs_output_path,
            data=output_content,
            encoding="utf-8",
            overwrite=True,
        )

        output_status = client.status(
            hdfs_output_path,
            strict=True,
        )

        print(
            "[成功] HDFS结果已写入，"
            f"大小={output_status['length']}字节，"
            f"所有者={output_status['owner']}"
        )

        print("\n[6/6] 回读HDFS结果并校验")

        with client.read(
            hdfs_output_path,
            encoding="utf-8",
        ) as reader:
            hdfs_output_content = reader.read()

        if hdfs_output_content != output_content:
            raise RuntimeError(
                "本地聚合结果与HDFS回读结果不一致"
            )

        print("[成功] 本地结果与HDFS结果完全一致")

        print("\n========================================")
        print("[全部成功] HDFS订单聚合完成")
        print(f"[有效订单] {valid_rows}")
        print(f"[用户数量] {len(statistics)}")
        print(f"[HDFS结果] {hdfs_output_path}")
        print(f"[本地结果] {local_output_path}")
        print("========================================")

        return 0

    except ValueError as exc:
        print(
            f"[数据错误] {exc}",
            file=sys.stderr,
        )
        return 3

    except HdfsError as exc:
        print(
            f"[HDFS错误] {exc}",
            file=sys.stderr,
        )
        return 4

    except (OSError, RuntimeError) as exc:
        print(
            f"[运行错误] {exc}",
            file=sys.stderr,
        )
        return 5


if __name__ == "__main__":
    raise SystemExit(main())
