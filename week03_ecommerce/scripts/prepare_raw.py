#!/usr/bin/env python3

from pathlib import Path
import sys

import pandas as pd


# 获取项目根目录。
# 当前文件位于 scripts/prepare_raw.py：
# parent 是 scripts，parent.parent 才是项目根目录。
PROJECT_DIR = Path(__file__).resolve().parent.parent

# 原始 Excel 文件。
SOURCE_FILE = (
    PROJECT_DIR
    / "data"
    / "source"
    / "Online Retail.xlsx"
)

# 转换后的完整 CSV 文件。
RAW_FILE = (
    PROJECT_DIR
    / "data"
    / "raw"
    / "online_retail.csv"
)

# 用于 GitHub 和快速测试的 1000 行样例。
SAMPLE_FILE = (
    PROJECT_DIR
    / "data"
    / "sample"
    / "online_retail_sample.csv"
)

# 原始字段名与标准字段名的对应关系。
COLUMN_MAPPING = {
    "InvoiceNo": "invoice_no",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "UnitPrice": "unit_price",
    "CustomerID": "customer_id",
    "Country": "country",
}


def main() -> None:
    """将原始 Excel 转换为标准 CSV。"""

    print("=" * 70)
    print("第三周 Day 2：Excel 转 CSV")
    print("=" * 70)

    # 1. 检查源文件是否存在。
    if not SOURCE_FILE.exists():
        print(
            f"错误：源文件不存在：{SOURCE_FILE}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"项目目录：{PROJECT_DIR}")
    print(f"源文件：{SOURCE_FILE}")
    print(
        f"源文件大小："
        f"{SOURCE_FILE.stat().st_size / 1024 / 1024:.2f} MB"
    )

    # 2. 确保输出目录存在。
    RAW_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    SAMPLE_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # 3. 读取 Excel。
    #
    # 订单号、商品编号和客户编号不能作为普通数字随意处理，
    # 因此明确使用字符串类型。
    print("\n[1/5] 正在读取 Excel 数据……")

    try:
        dataframe = pd.read_excel(
            SOURCE_FILE,
            engine="openpyxl",
            dtype={
                "InvoiceNo": "string",
                "StockCode": "string",
                "Description": "string",
                "CustomerID": "string",
                "Country": "string",
            },
        )
    except Exception as error:
        print(
            f"错误：Excel 读取失败：{error}",
            file=sys.stderr,
        )
        sys.exit(1)

    print("Excel 读取完成")
    print(f"原始记录数：{len(dataframe):,}")
    print(f"原始字段数：{len(dataframe.columns)}")

    # 4. 检查必需字段。
    print("\n[2/5] 正在检查字段结构……")

    missing_columns = [
        column
        for column in COLUMN_MAPPING
        if column not in dataframe.columns
    ]

    if missing_columns:
        print(
            f"错误：缺少字段：{missing_columns}",
            file=sys.stderr,
        )
        print(
            f"实际字段：{list(dataframe.columns)}",
            file=sys.stderr,
        )
        sys.exit(1)

    print("必需字段全部存在")

    # 5. 重命名字段。
    print("\n[3/5] 正在统一字段名称……")

    dataframe = dataframe.rename(
        columns=COLUMN_MAPPING
    )

    # 只保留并按照指定顺序排列这 8 个字段。
    dataframe = dataframe[
        list(COLUMN_MAPPING.values())
    ]

    print("字段统一完成：")

    for column in dataframe.columns:
        print(f"  - {column}")

    # 6. 输出完整 CSV。
    print("\n[4/5] 正在输出完整 CSV……")

    dataframe.to_csv(
        RAW_FILE,
        index=False,
        encoding="utf-8",
        date_format="%Y-%m-%d %H:%M:%S",
    )

    print(f"完整 CSV：{RAW_FILE}")
    print(
        f"完整 CSV 大小："
        f"{RAW_FILE.stat().st_size / 1024 / 1024:.2f} MB"
    )

    # 7. 输出前 1000 行样例。
    print("\n[5/5] 正在输出 1000 行样例……")

    sample_dataframe = dataframe.head(1000)

    sample_dataframe.to_csv(
        SAMPLE_FILE,
        index=False,
        encoding="utf-8",
        date_format="%Y-%m-%d %H:%M:%S",
    )

    print(f"样例 CSV：{SAMPLE_FILE}")
    print(f"样例记录数：{len(sample_dataframe):,}")

    print("\n" + "=" * 70)
    print("转换完成")
    print(f"完整数据记录数：{len(dataframe):,}")
    print(f"字段数：{len(dataframe.columns)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
