import os 
import pandas as pd

from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Set, Tuple, Optional

def load_dividens(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "dividends_history.csv"
    if not path.exists():
        raise FileNotFoundError(f"Dividends file not found: {path}")

    df = pd.read_csv(
        path,
        dtype={
            "symbol": "string",
            "dividends": "float64",
        },
        parse_dates=["ex_dividend_date"],
        skip_blank_lines=True,
    )
    return df

def prepare_dividends_for_year(div_history_df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Filter dividends by ex_dividend_date year and sort.
    """
    if div_history_df is None or div_history_df.empty:
        return pd.DataFrame(columns=["symbol", "ex_dividend_date", "dividends"])

    df = div_history_df.copy()
    df["ex_dividend_date"] = pd.to_datetime(df["ex_dividend_date"]).dt.normalize()
    df = df[df["ex_dividend_date"].dt.year == year].copy()
    df = df.sort_values(["symbol", "ex_dividend_date"]).reset_index(drop=True)
    return df

def build_needed_snapshot_map(div_df_year: pd.DataFrame) -> list[pd.Timestamp]:

    if div_df_year is None or div_df_year.empty:
        return []
    
    df = div_df_year.copy()
    df["ex_dividend_date"] = pd.to_datetime(df["ex_dividend_date"]).dt.normalize()

    snapshot_dates = (
        df["ex_dividend_date"] - pd.Timedelta(days=1)
    ).dt.normalize().unique().tolist()

    return sorted(snapshot_dates)


def compute_dividend_ledger(
    div_df_year: pd.DataFrame,
    snapshots: Dict[pd.Timestamp, Dict[str, int]],
) -> pd.DataFrame:
    
    cols = [
        "symbol",
        "ex_dividend_date",
        "snapshot_date",
        "eligible_qty",
        "dividends_per_share",
        "dividend_amount",
    ]

    if div_df_year is None or div_df_year.empty:
        return pd.DataFrame(columns=cols)

    df = div_df_year.copy()
    df["symbol"] = df["symbol"].astype("string").str.strip()
    df["ex_dividend_date"] = pd.to_datetime(df["ex_dividend_date"]).dt.normalize()

    records = []

    for r in df.itertuples(index=False):
        symbol = str(r.symbol).strip()
        exd = pd.to_datetime(r.ex_dividend_date).normalize()
        snap_date = (exd - pd.Timedelta(days=1)).normalize()
        per_share = float(r.dividends)

        snap = snapshots.get(snap_date, {})
        eligible_qty = int(snap.get(symbol, 0))
        amount = eligible_qty * per_share

        records.append({
            "symbol": symbol,
            "ex_dividend_date": exd,
            "snapshot_date": snap_date,
            "eligible_qty": eligible_qty,
            "dividends_per_share": per_share,
            "dividend_amount": amount,
        })

    return pd.DataFrame(records, columns=cols)

def save_dividend_ledger(output_path: Path, dividend_ledger_df: pd.DataFrame) -> Path:
    """
    Save dividend ledger to CSV.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dividend_ledger_df.to_csv(output_path, index=False)
    return output_path

def _parse_any_date(x) -> pd.Timestamp:
    """
    Parse a date that may be:
      - 'YYYY/MM/DD'
      - 'YYYY-MM-DD'
      - Excel serial number (e.g., 45953)
    """
    if pd.isna(x):
        return pd.NaT

    s = str(x).strip()
    if not s:
        return pd.NaT

    # Excel serial number
    if s.isdigit():
        # Pandas uses 1899-12-30 as origin for Excel serial dates
        return pd.to_datetime(int(s), unit="D", origin="1899-12-30", errors="coerce")

    return pd.to_datetime(s, errors="coerce")

if __name__ == "__main__":

    data_dir = Path("data")
    year = 2023

    div_history_df = load_dividens(data_dir)
    div_df_year = prepare_dividends_for_year(div_history_df, year)
    print(f"Dividends for year {year}:")
    print(div_df_year)

    needed_dates_list = build_needed_snapshot_map(div_df_year)
    print("Needed snapshot list:")
    print(needed_dates_list)

    # fakce snapshots
    fake_snapshots = {

        pd.Timestamp("2023-01-29"): {"2330": 50, "0050": 100},
        pd.Timestamp("2023-06-14"): {"2330": 300, "0050": 500},
        pd.Timestamp("2023-06-16"): {"2330": 400, "0050": 600},
        pd.Timestamp("2023-09-13"): {"2330": 500, "0050": 900},
    }


    # compute ledger
    ledger = compute_dividend_ledger(div_df_year,fake_snapshots)
    print("Dividend Ledger:")
    print(ledger)



        
    

