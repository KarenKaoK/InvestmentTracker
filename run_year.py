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
)


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

    inventory, realized_pnl_df = apply_trades_fifo(trades_df, inventories)
    print("Updated Inventory:", inventory)

    print("Realized PnL DataFrame:")
    print(realized_pnl_df)

    save_inventories(data_dir, year, inventories)
    save_realized_pnl(data_dir, year, realized_pnl_df)

    

    # =======================




if __name__ == "__main__":
    main()