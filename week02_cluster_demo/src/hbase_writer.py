#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import happybase
from thrift.transport.TTransport import TTransportException


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="将用户订单聚合结果写入HBase"
    )

    parser.add_argument(
        "--summary-file",
        required=True,
        help="本地聚合CSV文件",
    )

    parser.add_argument(
        "--biz-date",
        required=True,
        help="业务日期，格式为 YYYY-MM-DD",
    )

    return parser.parse_args()


def validate_date(date_text: str) -> str:
    try:
        date_value = datetime.strptime(
            date_text,
            "%Y-%m-%d",
        )
    except ValueError as exc:
        raise ValueError(
            f"日期格式错误：{date_text}，必须使用 YYYY-MM-DD"
        ) from exc

    return date_value.strftime("%Y%m%d")


def get_required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()

    if not value:
        raise RuntimeError(
            f"缺少环境变量：{name}"
        )

    return value


def decode_text(value: bytes | str) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")

    return str(value)


def create_connection() -> happybase.Connection:
    host = get_required_env("HBASE_HOST")
    port = int(get_required_env("HBASE_PORT"))

    connection = happybase.Connection(
        host=host,
        port=port,
        timeout=5000,
        autoconnect=False,
        transport="buffered",
        protocol="binary",
    )

    connection.open()
    return connection


def ensure_table(
    connection: happybase.Connection,
    table_name: str,
    family_name: str,
) -> None:
    existing_tables = {
        decode_text(name)
        for name in connection.tables()
    }

    if table_name not in existing_tables:
        print(
            f"[建表] 表不存在，正在创建：{table_name}"
        )

        connection.create_table(
            table_name,
            {
                family_name: {
                    "max_versions": 1,
                }
            },
        )

        print(
            f"[成功] 已创建表：{table_name}，"
            f"列族：{family_name}"
        )
    else:
        print(f"[检查] 表已经存在：{table_name}")

    if not connection.is_table_enabled(table_name):
        print(f"[启用] 表当前被禁用：{table_name}")
        connection.enable_table(table_name)
        print(f"[成功] 表已启用：{table_name}")


def read_summary_file(
    summary_file: Path,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    with summary_file.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as file:
        reader = csv.DictReader(file)

        required_fields = {
            "user_id",
            "order_count",
            "total_amount",
        }

        actual_fields = set(reader.fieldnames or [])
        missing_fields = required_fields - actual_fields

        if missing_fields:
            raise ValueError(
                "聚合CSV缺少字段："
                + ", ".join(sorted(missing_fields))
            )

        for line_number, row in enumerate(
            reader,
            start=2,
        ):
            user_id = (row.get("user_id") or "").strip()
            order_count_text = (
                row.get("order_count") or ""
            ).strip()
            total_amount_text = (
                row.get("total_amount") or ""
            ).strip()

            if not user_id:
                raise ValueError(
                    f"第{line_number}行 user_id 为空"
                )

            try:
                order_count = int(order_count_text)
            except ValueError as exc:
                raise ValueError(
                    f"第{line_number}行订单数量非法："
                    f"{order_count_text}"
                ) from exc

            if order_count < 0:
                raise ValueError(
                    f"第{line_number}行订单数量不能为负数"
                )

            try:
                total_amount = Decimal(
                    total_amount_text
                )
            except InvalidOperation as exc:
                raise ValueError(
                    f"第{line_number}行金额非法："
                    f"{total_amount_text}"
                ) from exc

            if total_amount < 0:
                raise ValueError(
                    f"第{line_number}行金额不能为负数"
                )

            rows.append(
                {
                    "user_id": user_id,
                    "order_count": str(order_count),
                    "total_amount": format(
                        total_amount,
                        ".2f",
                    ),
                }
            )

    if not rows:
        raise ValueError("聚合CSV中没有数据")

    return rows


def main() -> int:
    args = parse_args()

    try:
        summary_file = Path(
            args.summary_file
        ).expanduser().resolve()

        if not summary_file.is_file():
            print(
                f"[错误] 聚合文件不存在：{summary_file}",
                file=sys.stderr,
            )
            return 1

        date_key = validate_date(args.biz_date)

        table_name = get_required_env(
            "HBASE_TABLE"
        )
        family_name = get_required_env(
            "HBASE_FAMILY"
        )

        print("========================================")
        print("用户订单统计写入HBase")
        print("========================================")
        print(f"[输入文件] {summary_file}")
        print(f"[业务日期] {args.biz_date}")
        print(
            f"[HBase地址] "
            f"{get_required_env('HBASE_HOST')}:"
            f"{get_required_env('HBASE_PORT')}"
        )
        print(f"[HBase表] {table_name}")
        print(f"[列族] {family_name}")

        print("\n[1/5] 读取并检查聚合CSV")

        summary_rows = read_summary_file(
            summary_file
        )

        print(
            f"[成功] 读取用户记录："
            f"{len(summary_rows)}条"
        )

        print("\n[2/5] 连接HBase Thrift")

        connection = create_connection()

        try:
            print("[成功] HBase连接成功")

            print("\n[3/5] 检查HBase表")

            ensure_table(
                connection=connection,
                table_name=table_name,
                family_name=family_name,
            )

            table = connection.table(table_name)

            print("\n[4/5] 批量写入HBase")

            update_time = datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            with table.batch(
                batch_size=100
            ) as batch:
                for row in summary_rows:
                    user_id = row["user_id"]

                    row_key = (
                        f"{date_key}#{user_id}"
                    ).encode("utf-8")

                    batch.put(
                        row_key,
                        {
                            (
                                f"{family_name}:user_id"
                            ).encode("utf-8"):
                                user_id.encode("utf-8"),

                            (
                                f"{family_name}:order_count"
                            ).encode("utf-8"):
                                row["order_count"].encode(
                                    "utf-8"
                                ),

                            (
                                f"{family_name}:total_amount"
                            ).encode("utf-8"):
                                row["total_amount"].encode(
                                    "utf-8"
                                ),

                            (
                                f"{family_name}:biz_date"
                            ).encode("utf-8"):
                                args.biz_date.encode("utf-8"),

                            (
                                f"{family_name}:update_time"
                            ).encode("utf-8"):
                                update_time.encode("utf-8"),
                        },
                    )

                    print(
                        f"[准备写入] "
                        f"{row_key.decode('utf-8')}"
                    )

            print(
                f"[成功] 已批量写入"
                f"{len(summary_rows)}条记录"
            )

            print("\n[5/5] 扫描本次业务日期数据")

            row_prefix = (
                f"{date_key}#"
            ).encode("utf-8")

            scanned_rows = 0

            for row_key, data in table.scan(
                row_prefix=row_prefix
            ):
                scanned_rows += 1

                decoded_data = {
                    decode_text(column):
                    decode_text(value)
                    for column, value in data.items()
                }

                print(
                    f"RowKey={decode_text(row_key)}"
                )

                for column in sorted(decoded_data):
                    print(
                        f"  {column}="
                        f"{decoded_data[column]}"
                    )

            if scanned_rows != len(summary_rows):
                raise RuntimeError(
                    "扫描记录数与写入记录数不一致："
                    f"写入={len(summary_rows)}，"
                    f"扫描={scanned_rows}"
                )

            print("\n========================================")
            print("[全部成功] 聚合结果已经写入HBase")
            print(f"[写入记录] {len(summary_rows)}")
            print(f"[表名] {table_name}")
            print(f"[RowKey前缀] {date_key}#")
            print("========================================")

        finally:
            connection.close()
            print("[连接] HBase连接已关闭")

        return 0

    except ValueError as exc:
        print(
            f"[数据错误] {exc}",
            file=sys.stderr,
        )
        return 2

    except TTransportException as exc:
        print(
            f"[Thrift连接错误] {exc}",
            file=sys.stderr,
        )
        return 3

    except RuntimeError as exc:
        print(
            f"[运行错误] {exc}",
            file=sys.stderr,
        )
        return 4

    except Exception as exc:
        print(
            f"[未知错误] "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 5


if __name__ == "__main__":
    raise SystemExit(main())
