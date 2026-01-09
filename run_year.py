import os
import argparse
import pandas as pd 
from pathlib import Path

from src.bootstrap import ensure_opening_data, build_opening_tables, save_opening_tables
from src.engine_fifo import (
    load_inventory,
    load_trades,
    inventory_df_to_queues,
    apply_trades_fifo,
    save_inventories,
    save_realized_pnl,
    load_actions,
)
from src.dividends import (
    load_dividens,
    prepare_dividends_for_year,
    build_needed_snapshot_map,
    compute_dividend_ledger,
    save_dividend_ledger,
)
from src.snapshots import SnapshotCollector



def main():
    parser = argparse.ArgumentParser(
        description="Process investment data for a given year."
    )

    # process year argument
    parser.add_argument(
        "year",
        type=int,
        help="Processing year (e.g. 2025)"
    )

    # flag : is start year 
    parser.add_argument(
        "--is-start",
        action="store_true",
        help="Indicate this year is the starting year (load opening snapshots)"
    )

    parser.add_argument(
        "--data-dir",
        type=str,
        default="data",
        help="Directory to store opening tables (default: ./data)"
    )

    args = parser.parse_args()

    year = args.year
    is_start_year = args.is_start
    data_dir = Path(args.data_dir)

    print(f"Processing year: {year}")
    print(f"Is start year: {is_start_year}")

    # === boostrap === 
    if is_start_year:
        inv_df, pnl_df, div_df = build_opening_tables()
        paths = save_opening_tables(data_dir, year, inv_df, pnl_df, div_df)
        print("Opening tables generated:")
        for k, v in paths.items():
            print(f"  - {k}: {v}")
    else:
        paths = ensure_opening_data(data_dir, year)
        print("Opening tables verified:")
        for k, v in paths.items():
            print(f"  - {k}: {v}")
    # ==================

    # === run engine fifo ===
    inv_df = load_inventory(data_dir, year)
    trades_df = load_trades(data_dir, year)

    inventories = inventory_df_to_queues(inv_df)

    for symbol, queue in inventories.items():
        print(f"Inventory for {symbol}: {list(queue)}\n")

    actions_df = load_actions(data_dir, year)  

    # --- dividends: prepare snapshot dates for this year ---
    try:
        div_history_df = load_dividens(data_dir)
        div_df_year = prepare_dividends_for_year(div_history_df, year)
        snapshot_dates = build_needed_snapshot_map(div_df_year)  # list[pd.Timestamp]
    except FileNotFoundError:
        div_df_year = pd.DataFrame(columns=["symbol", "ex_dividend_date", "dividends"])
        snapshot_dates = []

    collector = SnapshotCollector(snapshot_dates)

    inventory, realized_pnl_df, snapshots = apply_trades_fifo(
        trades_df,
        inventories,
        actions_df,
        year,
        snapshot_collector=collector,
    )

    print("Updated Inventory:", inventory)
    print("Realized PnL DataFrame:")
    print(realized_pnl_df)

    save_inventories(data_dir, year, inventories)
    save_realized_pnl(data_dir, year, realized_pnl_df)

    # --- dividends: compute ledger using snapshots ---
    if snapshots is not None and div_df_year is not None and not div_df_year.empty:
        dividend_ledger_df = compute_dividend_ledger(div_df_year, snapshots)
        out_path = save_dividend_ledger(data_dir / f"{year}" / "dividends.csv", dividend_ledger_df)
        print(f"!!! Saved dividends ledger to {out_path}")

    print('finished')
    

    # =======================




if __name__ == "__main__":
    main()