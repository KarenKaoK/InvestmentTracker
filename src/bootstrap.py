import os
import pandas as pd 
from pathlib import Path

def opening_path(data_dir, year:int) -> dict:
    return {
        "inventory": data_dir/f"{year}"/f"inventory.csv"
    }

def build_opening_tables():

    # inventory.csv
    inv_df = pd.DataFrame({
        "transaction_date": pd.Series(dtype="datetime64[ns]"),
        "stock_symbol": pd.Series(dtype="string"),
        "stock_name": pd.Series(dtype="string"),
        "qty": pd.Series(dtype="int64"),
        "": pd.Series(dtype="float64"),
    })
   
    
    # realized_pnl.csv
    pnl_df = pd.DataFrame({
        "transaction_date": pd.Series(dtype="datetime64[ns]"),
        "stock_symbol": pd.Series(dtype="string"),
        "stock_name": pd.Series(dtype="string"),
        "qty": pd.Series(dtype="int64"),
        "cost_basis": pd.Series(dtype="float64"),
        "sell_proceeds": pd.Series(dtype="float64"),
        "realized_pnl": pd.Series(dtype="float64"),
    })
    
    # dividends.csv
    div_df = pd.DataFrame({
        "ex_dividend_date": pd.Series(dtype="datetime64[ns]"),
        "stock_symbol": pd.Series(dtype="string"),
        "stock_name": pd.Series(dtype="string"),
        "dividend_per_share": pd.Series(dtype="float64"),
        "current_qty": pd.Series(dtype="int64"),
        "total_dividend": pd.Series(dtype="float64")
    })

    return inv_df, pnl_df, div_df

def save_opening_tables(data_dir: Path, year:int,
                        inv_df, pnl_df, div_df):
    paths = opening_path(data_dir, year)
    inv_df.to_csv(paths["inventory"], index=False)
    return paths

def ensure_opening_data(data_dir: Path, year:int)-> dict:
    paths = opening_path(data_dir, year)
    missing = [k for k, path in paths.items() if not path.exists()]
    if missing:
        msg = ", ".join(f"{k}={paths[k].name}" for k in missing)
        raise FileNotFoundError(
            f"Missing opening tables for year {year}: {msg}. "
            f"If this is the start year, run with --is-start to generate them."
        )
    return paths
   