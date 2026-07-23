#!/usr/bin/env python3

from pathlib import Path
import sys

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent.parent
SOURCE_FILE = PROJECT_DIR / "data" / "source" / "Online Retail.xlsx"


def main() -> None:
    """检查原始 Excel 文件是否可以正常读取。"""

    if not SOURCE_FILE.exists():
        print(f"错误：找不到原始文件：{SOURCE_FILE}", file=sys.stderr)
        sys.exit(1)

    print("=" * 70)
    print("第三周 Day 1：原始数据检查")
    print("=" * 70)
    print(f"项目目录：{PROJECT_DIR}")
    print(f"原始文件：{SOURCE_FILE}")
    print(f"文件大小：{SOURCE_FILE.stat().st_size / 1024 / 1024:.2f} MB")

    try:
        excel_file = pd.ExcelFile(
            SOURCE_FILE,
            engine="openpyxl",
        )
    except Exception as error:
        print(f"错误：无法打开 Excel 文件：{error}", file=sys.stderr)
        sys.exit(1)

    print(f"工作表：{excel_file.sheet_names}")

    try:
        dataframe = pd.read_excel(
            SOURCE_FILE,
            sheet_name=0,
            engine="openpyxl",
            nrows=10,
        )
    except Exception as error:
        print(f"错误：读取数据失败：{error}", file=sys.stderr)
        sys.exit(1)

    print(f"字段数量：{len(dataframe.columns)}")
    print("字段名称：")

    for index, column in enumerate(dataframe.columns, start=1):
        print(f"  {index}. {column}")

    print("\n前 5 行数据：")
    print(dataframe.head(5).to_string(index=False))

    print("\n前 10 行字段类型：")
    print(dataframe.dtypes)

    expected_columns = {
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    }

    actual_columns = set(dataframe.columns)
    missing_columns = expected_columns - actual_columns
    extra_columns = actual_columns - expected_columns

    print("\n字段检查：")

    if not missing_columns:
        print("  必需字段：全部存在")
    else:
        print(f"  缺少字段：{sorted(missing_columns)}")

    if extra_columns:
        print(f"  额外字段：{sorted(extra_columns)}")
    else:
        print("  额外字段：无")

    if missing_columns:
        print("\n检查失败：数据结构不符合预期。", file=sys.stderr)
        sys.exit(1)

    print("\n检查成功：原始数据可以用于 Day 2 格式转换。")


if __name__ == "__main__":
    main()
