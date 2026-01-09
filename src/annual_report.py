# src/annual_report.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


# -------------------------
# Loaders
# -------------------------

def load_close_prices(data_dir: Path) -> pd.DataFrame:

    path = data_dir / "close_price.csv"
    if not path.exists():
        raise FileNotFoundError(f"close_price.csv not found: {path}")

    df = pd.read_csv(
        path,
        dtype={"symbol": "string", "close_price": "float64"},
        parse_dates=["date"],
        skip_blank_lines=True,
    )
    df["symbol"] = df["symbol"].astype("string").str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()

    bad = df[df["date"].isna() | df["close_price"].isna() | df["symbol"].isna()]
    if not bad.empty:
        raise ValueError(f"close_price.csv has invalid rows:\n{bad}")

    return df


def load_realized_pnl(data_dir: Path, year: int) -> pd.DataFrame:
    path = data_dir / f"{year}" / "realized_pnl.csv"
    if not path.exists():
        return pd.DataFrame(columns=["transaction_date", "stock_symbol", "realized_pnl"])

    df = pd.read_csv(
        path,
        dtype={"stock_symbol": "string"},   
        parse_dates=["transaction_date"],
        skip_blank_lines=True,
    )
    return df


def load_dividend_ledger(data_dir: Path, year: int) -> pd.DataFrame:

    path = data_dir / f"{year}" / "dividends.csv"
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "dividend_amount"])

    df = pd.read_csv(
        path,
        dtype={"symbol": "string"},
        parse_dates=["ex_dividend_date", "snapshot_date"],
        skip_blank_lines=True,
    )
    df["symbol"] = df["symbol"].astype("string").str.strip()
    if "dividend_amount" not in df.columns:
        df["dividend_amount"] = 0.0
    return df


def load_year_end_inventory(data_dir: Path, year: int) -> pd.DataFrame:

    path = data_dir / f"{year + 1}" / "inventory.csv"
    if not path.exists():
        return pd.DataFrame(columns=["transaction_date", "stock_symbol", "qty", "price"])

    df = pd.read_csv(
        path,
        dtype={"stock_symbol": "string", "qty": "Int64", "price": "float64"},
        parse_dates=["transaction_date"],
        skip_blank_lines=True,
    )
    df["stock_symbol"] = df["stock_symbol"].astype("string").str.strip()
    return df


# -------------------------
# Transform
# -------------------------

def get_year_end_prices(close_prices_df: pd.DataFrame, year: int) -> pd.DataFrame:

    df = close_prices_df.copy()
    df = df[df["date"].dt.year == year].copy()
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    return df


def summarize_inventory_lots(inv_lots_df: pd.DataFrame) -> pd.DataFrame:

    if inv_lots_df is None or inv_lots_df.empty:
        return pd.DataFrame(columns=["symbol", "year_end_qty", "total_cost", "avg_cost"])

    df = inv_lots_df.copy()

    required = {"stock_symbol", "qty", "price"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"inventory.csv missing columns: {sorted(missing)}")

    df["stock_symbol"] = df["stock_symbol"].astype("string")
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df = df.dropna(subset=["stock_symbol", "qty", "price"]).copy()
    df["qty"] = df["qty"].astype("int64")
    df["price"] = df["price"].astype("float64")

    df["cost"] = df["qty"] * df["price"]

    out = (
        df.groupby("stock_symbol", as_index=False)
          .agg(
              year_end_qty=("qty", "sum"),
              total_cost=("cost", "sum"),
          )
          .rename(columns={"stock_symbol": "symbol"})
    )


    out["avg_cost"] = (out["total_cost"] / out["year_end_qty"]).where(out["year_end_qty"] != 0, 0.0)

    out["total_cost"] = out["total_cost"].round(2)
    out["avg_cost"] = out["avg_cost"].round(6)

    out = out[out["year_end_qty"] != 0].sort_values(["symbol"]).reset_index(drop=True)
    return out



def summarize_realized(realized_df: pd.DataFrame) -> Tuple[pd.DataFrame, float]:

    if realized_df is None or realized_df.empty:
        empty = pd.DataFrame(columns=["symbol", "realized_pnl"])
        return empty, 0.0

    df = realized_df.copy()
    if "stock_symbol" not in df.columns or "realized_pnl" not in df.columns:
        empty = pd.DataFrame(columns=["symbol", "realized_pnl"])
        return empty, 0.0

    df["stock_symbol"] = df["stock_symbol"].astype("string").str.strip()
    df["realized_pnl"] = pd.to_numeric(df["realized_pnl"], errors="coerce").fillna(0.0)

    by_symbol = df.groupby("stock_symbol", as_index=False)["realized_pnl"].sum()
    by_symbol = by_symbol.rename(columns={"stock_symbol": "symbol"})
    by_symbol["realized_pnl"] = by_symbol["realized_pnl"].round(2)

    total = float(by_symbol["realized_pnl"].sum()) if not by_symbol.empty else 0.0

    return by_symbol.sort_values(["symbol"]).reset_index(drop=True), round(total, 2)


def summarize_dividends(div_df: pd.DataFrame) -> Tuple[pd.DataFrame, float]:

    if div_df is None or div_df.empty:
        empty = pd.DataFrame(columns=["symbol", "dividend_amount"])
        return empty, 0.0

    df = div_df.copy()
    if "symbol" not in df.columns:
        empty = pd.DataFrame(columns=["symbol", "dividend_amount"])
        return empty, 0.0

    if "dividend_amount" not in df.columns:
        df["dividend_amount"] = 0.0

    df["symbol"] = df["symbol"].astype("string").str.strip()
    df["dividend_amount"] = pd.to_numeric(df["dividend_amount"], errors="coerce").fillna(0.0)

    by_symbol = df.groupby("symbol", as_index=False)["dividend_amount"].sum()
    by_symbol["dividend_amount"] = by_symbol["dividend_amount"].round(2)

    total = float(by_symbol["dividend_amount"].sum()) if not by_symbol.empty else 0.0
    return by_symbol.sort_values(["symbol"]).reset_index(drop=True), round(total, 2)


# -------------------------
# Report builder
# -------------------------

def build_annual_report(
    data_dir: Path,
    year: int,
) -> dict[str, pd.DataFrame]:

    close_prices_df = load_close_prices(data_dir)
    year_end_prices_df = get_year_end_prices(close_prices_df, year)

    realized_df = load_realized_pnl(data_dir, year)
    div_ledger_df = load_dividend_ledger(data_dir, year)
    inv_lots_df = load_year_end_inventory(data_dir, year)

    holdings_df = summarize_inventory_lots(inv_lots_df)

    realized_by_symbol, realized_total = summarize_realized(realized_df)
    dividends_by_symbol, dividends_total = summarize_dividends(div_ledger_df)

    # merge holdings with prices to compute market value & unrealized pnl
    prices = year_end_prices_df.rename(columns={"close_price": "year_end_close"}).copy()
    prices = prices[["symbol", "date", "year_end_close"]].copy()

    holdings_with_price = holdings_df.merge(prices, on="symbol", how="left")

    holdings_with_price["year_end_market_value"] = (
        holdings_with_price["year_end_qty"] * holdings_with_price["year_end_close"]
    )
    holdings_with_price["unrealized_pnl"] = (
        holdings_with_price["year_end_market_value"] - holdings_with_price["total_cost"]
    )

    # keep nice rounding
    holdings_with_price["year_end_close"] = holdings_with_price["year_end_close"].round(4)
    holdings_with_price["year_end_market_value"] = holdings_with_price["year_end_market_value"].round(2)
    holdings_with_price["unrealized_pnl"] = holdings_with_price["unrealized_pnl"].round(2)

    unrealized_total = float(holdings_with_price["unrealized_pnl"].fillna(0.0).sum()) if not holdings_with_price.empty else 0.0
    unrealized_total = round(unrealized_total, 2)

    total_pnl = round(realized_total + dividends_total + unrealized_total, 2)

    # summary table
    summary_df = pd.DataFrame(
        [
            {"metric": "realized_pnl_total", "value": realized_total},
            {"metric": "dividends_total", "value": dividends_total},
            {"metric": "unrealized_pnl_total", "value": unrealized_total},
            {"metric": "total_pnl", "value": total_pnl},
        ]
    )

    # a merged by_symbol view (optional but useful)
    by_symbol = realized_by_symbol.merge(dividends_by_symbol, on="symbol", how="outer")
    by_symbol = by_symbol.merge(
        holdings_with_price[["symbol", "year_end_qty", "total_cost", "year_end_close", "year_end_market_value", "unrealized_pnl"]],
        on="symbol",
        how="outer",
    )
    for c in ["realized_pnl", "dividend_amount", "unrealized_pnl"]:
        if c in by_symbol.columns:
            by_symbol[c] = pd.to_numeric(by_symbol[c], errors="coerce").fillna(0.0)

    by_symbol["total_pnl"] = (by_symbol.get("realized_pnl", 0.0) + by_symbol.get("dividend_amount", 0.0) + by_symbol.get("unrealized_pnl", 0.0)).round(2)
    by_symbol = by_symbol.sort_values(["symbol"]).reset_index(drop=True)

    return {
        "summary": summary_df,
        "holdings_year_end": holdings_with_price.sort_values(["symbol"]).reset_index(drop=True),
        "realized_by_symbol": realized_by_symbol,
        "dividends_by_symbol": dividends_by_symbol,
        "by_symbol": by_symbol,
    }


def save_annual_report(
    data_dir: Path,
    year: int,
    report: dict[str, pd.DataFrame],
) -> dict[str, Path]:

    out_dir = data_dir / f"{year}"
    out_dir.mkdir(parents=True, exist_ok=True)

    html_path = out_dir / f"annual_report_{year}.html"
    xlsx_path = out_dir / f"annual_report_{year}.xlsx"

    # -------------------------
    # HTML
    # -------------------------
    def _df_to_html(df: pd.DataFrame) -> str:
        if df is None or df.empty:
            return "<p><em>(empty)</em></p>"
        return df.to_html(index=False, border=0, classes="tbl")

    html = f"""<!doctype html>
    <html lang="zh-Hant">
    <head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>{year} 年度投資報告</title>
    <style>
      body {{
        font-family: -apple-system, BlinkMacSystemFont, "PingFang TC",
                    "Noto Sans TC", "Microsoft JhengHei", Arial, sans-serif;
        margin: 24px;
        line-height: 1.6;
      }}
      h1 {{ margin-bottom: 4px; }}
      h2 {{ margin-top: 24px; }}
      .meta {{ color: #666; margin-top: 0; font-size: 14px; }}
      .section {{ margin-top: 28px; }}
      table.tbl {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
      .tbl th, .tbl td {{ border: 1px solid #ddd; padding: 8px; }}
      .tbl th {{ background: #f6f6f6; text-align: left; }}
    </style>
    </head>
    <body>

      <h1>{year} 年度投資報告</h1>
      <p class="meta">
        產生時間：{pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}
      </p>

      <div class="section">
        <h2>年度損益總覽</h2>
        <p class="meta">
          本年度投資績效彙總，包含已實現損益、股利收入與期末未實現損益。
        </p>
        {_df_to_html(report.get("summary"))}
      </div>

      <div class="section">
        <h2>標的別損益彙總</h2>
        <p class="meta">
          依股票代號彙整之年度損益明細（已實現損益＋股利＋期末未實現損益）。
        </p>
        {_df_to_html(report.get("by_symbol"))}
      </div>

      <div class="section">
        <h2>期末持股明細</h2>
        <p class="meta">
          截至 {year} 年底之持股數量、成本、期末市值與未實現損益。
        </p>
        {_df_to_html(report.get("holdings_year_end"))}
      </div>

      <div class="section">
        <h2>已實現損益（依標的）</h2>
        <p class="meta">
          本年度賣出交易所產生之已實現損益。
        </p>
        {_df_to_html(report.get("realized_by_symbol"))}
      </div>

      <div class="section">
        <h2>股利收入（依標的）</h2>
        <p class="meta">
          本年度除權息所取得之股利收入。
        </p>
        {_df_to_html(report.get("dividends_by_symbol"))}
      </div>

    </body>
    </html>
    """

    html_path.write_text(html, encoding="utf-8")

    # -------------------------
    # Excel (multi-sheet)
    # -------------------------
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:

        sheets = [
            ("summary", report.get("summary")),
            ("by_symbol", report.get("by_symbol")),
            ("holdings_year_end", report.get("holdings_year_end")),
            ("realized_by_symbol", report.get("realized_by_symbol")),
            ("dividends_by_symbol", report.get("dividends_by_symbol")),
        ]
        for sheet_name, df in sheets:
            if df is None:
                df = pd.DataFrame()
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)  

    return {"html": html_path, "xlsx": xlsx_path}

if __name__ == "__main__":

    data_dir = Path("data")
    year = 2025


    report = build_annual_report(data_dir, year)
    save_annual_report(data_dir, year,report)


