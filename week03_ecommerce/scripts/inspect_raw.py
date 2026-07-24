#!/usr/bin/env python3

from pathlib import Path
import sys

import pandas as pd


# 项目根目录：
# inspect_raw.py 位于 scripts 目录，
# parent.parent 表示返回项目根目录。
PROJECT_DIR = Path(__file__).resolve().parent.parent

# Day 2 生成的完整原始 CSV。
INPUT_FILE = (
    PROJECT_DIR
    / "data"
    / "raw"
    / "online_retail.csv"
)

# 本脚本生成的数据质量报告。
REPORT_FILE = (
    PROJECT_DIR
    / "docs"
    / "data_profile.md"
)

# 数据集必须包含的 8 个字段。
EXPECTED_COLUMNS = [
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "unit_price",
    "customer_id",
    "country",
]


def format_percent(count: int, total: int) -> str:
    """把数量转换为百分比字符串。"""

    if total == 0:
        return "0.00%"

    return f"{count / total * 100:.2f}%"


def main() -> None:
    """检查原始 CSV 的结构、空值、重复和业务异常。"""

    print("=" * 70)
    print("第三周 Day 3：原始数据质量检查")
    print("=" * 70)

    # ------------------------------------------------------------
    # 1. 检查输入文件
    # ------------------------------------------------------------

    if not INPUT_FILE.exists():
        print(
            f"错误：找不到原始 CSV：{INPUT_FILE}",
            file=sys.stderr,
        )
        print(
            "请先完成 Day 2，并运行："
            "python3 scripts/prepare_raw.py",
            file=sys.stderr,
        )
        sys.exit(1)

    file_size_mb = INPUT_FILE.stat().st_size / 1024 / 1024

    print(f"项目目录：{PROJECT_DIR}")
    print(f"输入文件：{INPUT_FILE}")
    print(f"文件大小：{file_size_mb:.2f} MB")

    # ------------------------------------------------------------
    # 2. 读取 CSV
    # ------------------------------------------------------------

    print("\n[1/6] 正在读取原始 CSV……")

    try:
        dataframe = pd.read_csv(
            INPUT_FILE,
            dtype={
                "invoice_no": "string",
                "stock_code": "string",
                "description": "string",
                "customer_id": "string",
                "country": "string",
            },
            low_memory=False,
        )
    except Exception as error:
        print(
            f"错误：CSV 读取失败：{error}",
            file=sys.stderr,
        )
        sys.exit(1)

    total_rows = len(dataframe)
    total_columns = len(dataframe.columns)

    print("CSV 读取完成")
    print(f"记录数：{total_rows:,}")
    print(f"字段数：{total_columns}")

    # ------------------------------------------------------------
    # 3. 检查字段
    # ------------------------------------------------------------

    print("\n[2/6] 正在检查字段结构……")

    actual_columns = list(dataframe.columns)

    missing_columns = [
        column
        for column in EXPECTED_COLUMNS
        if column not in actual_columns
    ]

    extra_columns = [
        column
        for column in actual_columns
        if column not in EXPECTED_COLUMNS
    ]

    if missing_columns:
        print(
            f"错误：缺少必需字段：{missing_columns}",
            file=sys.stderr,
        )
        print(
            f"实际字段：{actual_columns}",
            file=sys.stderr,
        )
        sys.exit(1)

    print("8 个必需字段全部存在")

    if extra_columns:
        print(f"发现额外字段：{extra_columns}")
    else:
        print("没有额外字段")

    # 重新排列字段顺序。
    dataframe = dataframe[EXPECTED_COLUMNS]

    # ------------------------------------------------------------
    # 4. 转换需要检查的数据类型
    # ------------------------------------------------------------

    print("\n[3/6] 正在检查并转换字段类型……")

    # errors="coerce" 表示：
    # 无法转换成数字的值会变成缺失值 NaN。
    dataframe["quantity"] = pd.to_numeric(
        dataframe["quantity"],
        errors="coerce",
    )

    dataframe["unit_price"] = pd.to_numeric(
        dataframe["unit_price"],
        errors="coerce",
    )

    # 无法识别的日期会变成 NaT。
    dataframe["invoice_date"] = pd.to_datetime(
        dataframe["invoice_date"],
        errors="coerce",
    )

    print("字段类型转换完成")
    print(dataframe.dtypes)

    # ------------------------------------------------------------
    # 5. 计算数据质量指标
    # ------------------------------------------------------------

    print("\n[4/6] 正在计算数据质量指标……")

    # 每个字段的空值数量。
    null_counts = dataframe.isna().sum()

    # 每个字段的空值比例。
    null_rates = dataframe.isna().mean() * 100

    # 完全重复的整行数据数量。
    duplicate_count = int(
        dataframe.duplicated().sum()
    )

    # 以 C 开头的订单号通常表示取消或退款订单。
    cancelled_mask = (
        dataframe["invoice_no"]
        .fillna("")
        .str.strip()
        .str.upper()
        .str.startswith("C")
    )

    cancelled_count = int(cancelled_mask.sum())

    # 数量异常。
    negative_quantity_count = int(
        (dataframe["quantity"] < 0).sum()
    )

    zero_quantity_count = int(
        (dataframe["quantity"] == 0).sum()
    )

    invalid_quantity_count = int(
        (dataframe["quantity"] <= 0).sum()
    )

    # 单价异常。
    negative_price_count = int(
        (dataframe["unit_price"] < 0).sum()
    )

    zero_price_count = int(
        (dataframe["unit_price"] == 0).sum()
    )

    invalid_price_count = int(
        (dataframe["unit_price"] <= 0).sum()
    )

    # 核心字段缺失情况。
    missing_invoice_count = int(
        dataframe["invoice_no"].isna().sum()
    )

    missing_customer_count = int(
        dataframe["customer_id"].isna().sum()
    )

    missing_description_count = int(
        dataframe["description"].isna().sum()
    )

    invalid_date_count = int(
        dataframe["invoice_date"].isna().sum()
    )

    # 日期范围。
    min_date = dataframe["invoice_date"].min()
    max_date = dataframe["invoice_date"].max()

    # 国家数量。
    country_count = int(
        dataframe["country"].nunique(dropna=True)
    )

    # 订单编号数量。
    invoice_count = int(
        dataframe["invoice_no"].nunique(dropna=True)
    )

    # 客户数量。
    customer_count = int(
        dataframe["customer_id"].nunique(dropna=True)
    )

    # 商品数量。
    product_count = int(
        dataframe["stock_code"].nunique(dropna=True)
    )

    # 每个国家的记录数量，取前 10。
    top_countries = (
        dataframe["country"]
        .fillna("缺失")
        .value_counts()
        .head(10)
    )

    print("数据质量指标计算完成")

    # ------------------------------------------------------------
    # 6. 生成 Markdown 报告
    # ------------------------------------------------------------

    print("\n[5/6] 正在生成数据质量报告……")

    report = [
        "# 原始数据质量报告",
        "",
        "## 一、文件信息",
        "",
        f"- 输入文件：`{INPUT_FILE.name}`",
        f"- 文件大小：{file_size_mb:.2f} MB",
        f"- 记录数：{total_rows:,}",
        f"- 字段数：{total_columns}",
        "",
        "## 二、数据规模",
        "",
        f"- 唯一订单编号数：{invoice_count:,}",
        f"- 唯一商品编号数：{product_count:,}",
        f"- 唯一客户编号数：{customer_count:,}",
        f"- 国家或地区数量：{country_count:,}",
        f"- 最早交易时间：{min_date}",
        f"- 最晚交易时间：{max_date}",
        "",
        "## 三、重复数据",
        "",
        f"- 完全重复记录数：{duplicate_count:,}",
        (
            f"- 完全重复记录比例："
            f"{format_percent(duplicate_count, total_rows)}"
        ),
        "",
        "## 四、退款和取消订单",
        "",
        f"- 退款或取消记录数：{cancelled_count:,}",
        (
            f"- 退款或取消记录比例："
            f"{format_percent(cancelled_count, total_rows)}"
        ),
        "",
        "说明：订单编号以 `C` 开头的记录，暂时标记为取消或退款记录。",
        "",
        "## 五、数量异常",
        "",
        f"- 数量小于 0：{negative_quantity_count:,}",
        f"- 数量等于 0：{zero_quantity_count:,}",
        f"- 数量小于等于 0：{invalid_quantity_count:,}",
        (
            f"- 数量异常比例："
            f"{format_percent(invalid_quantity_count, total_rows)}"
        ),
        "",
        "## 六、价格异常",
        "",
        f"- 单价小于 0：{negative_price_count:,}",
        f"- 单价等于 0：{zero_price_count:,}",
        f"- 单价小于等于 0：{invalid_price_count:,}",
        (
            f"- 单价异常比例："
            f"{format_percent(invalid_price_count, total_rows)}"
        ),
        "",
        "## 七、核心字段缺失",
        "",
        f"- 订单编号缺失：{missing_invoice_count:,}",
        f"- 商品描述缺失：{missing_description_count:,}",
        f"- 客户编号缺失：{missing_customer_count:,}",
        f"- 无法识别的日期：{invalid_date_count:,}",
        "",
        "## 八、全部字段空值统计",
        "",
        "| 字段 | 空值数量 | 空值比例 |",
        "|---|---:|---:|",
    ]

    for column in EXPECTED_COLUMNS:
        count = int(null_counts[column])
        rate = float(null_rates[column])

        report.append(
            f"| {column} | {count:,} | {rate:.2f}% |"
        )

    report.extend([
        "",
        "## 九、记录数量最多的前 10 个国家或地区",
        "",
        "| 排名 | 国家或地区 | 记录数 |",
        "|---:|---|---:|",
    ])

    for rank, (country, count) in enumerate(
        top_countries.items(),
        start=1,
    ):
        report.append(
            f"| {rank} | {country} | {int(count):,} |"
        )

    report.extend([
        "",
        "## 十、第四周建议处理规则",
        "",
        "1. 使用明确的 PySpark Schema 读取数据。",
        "2. 将订单时间转换为 TimestampType。",
        "3. 标记订单编号以 C 开头的退款或取消记录。",
        "4. 对数量小于等于 0 的记录进行分类处理。",
        "5. 对单价小于等于 0 的记录进行分类处理。",
        "6. 删除或单独保存完全重复记录。",
        "7. 客户编号缺失的数据不能直接用于客户分析。",
        "8. 商品描述缺失的数据需要单独标记。",
        "9. 原始数据不能被覆盖，清洗结果写入新目录。",
        "",
        "## 十一、处理说明",
        "",
        "- 本报告只检查和记录数据问题。",
        "- 本脚本不会删除、覆盖或修改原始 CSV。",
        "- 第四周再使用 PySpark 实现正式清洗。",
        "",
    ])

    REPORT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    REPORT_FILE.write_text(
        "\n".join(report),
        encoding="utf-8",
    )

    print(f"报告已写入：{REPORT_FILE}")

    # ------------------------------------------------------------
    # 7. 在终端打印核心结果
    # ------------------------------------------------------------

    print("\n[6/6] 核心检查结果")
    print("-" * 70)
    print(f"记录数：{total_rows:,}")
    print(f"字段数：{total_columns}")
    print(f"重复记录：{duplicate_count:,}")
    print(f"退款或取消记录：{cancelled_count:,}")
    print(f"数量小于等于 0：{invalid_quantity_count:,}")
    print(f"单价小于等于 0：{invalid_price_count:,}")
    print(f"客户编号缺失：{missing_customer_count:,}")
    print(f"商品描述缺失：{missing_description_count:,}")
    print(f"无法识别的日期：{invalid_date_count:,}")
    print(f"日期范围：{min_date} 至 {max_date}")
    print(f"国家或地区数量：{country_count:,}")
    print("-" * 70)
    print("检查完成：原始数据未被修改。")
    print(f"详细报告：{REPORT_FILE}")


if __name__ == "__main__":
    main()
