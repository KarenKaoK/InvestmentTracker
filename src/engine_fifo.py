import os
import pandas as pd
from pathlib import Path
from collections import deque, defaultdict
from typing import Dict, Deque, List, Optional, Tuple
from dataclasses import dataclass

# NOTE:
# This file integrates the snapshot + dividends workflow.
# - snapshots.py provides SnapshotCollector (date-driven inventory snapshots)
# - dividends.py provides dividend history parsing and ledger computation

try:
    # If running as a package (recommended)
    from src.snapshots import SnapshotCollector
    from src.dividends import (
        load_dividens,
        prepare_dividends_for_year,
        build_needed_snapshot_map,
        compute_dividend_ledger,
        save_dividend_ledger,
    )
except Exception:
    # If running as plain scripts in the same folder
    from snapshots import SnapshotCollector
    from dividends import (
        load_dividens,
        prepare_dividends_for_year,
        build_needed_snapshot_map,
        compute_dividend_ledger,
        save_dividend_ledger,
    )

@dataclass
class Lot:
    qty: int
    price: float
    date: pd.Timestamp

def load_inventory(data_dir: Path, year: int) -> pd.DataFrame:
    path = data_dir / f"{year}" / "inventory.csv"
    if not path.exists():
        raise FileNotFoundError(f"Inventory file not found: {path}")

    df = pd.read_csv(
        path,
        dtype={
            "stock_symbol": "string",
            "qty": "Int64",
            "price": "float64",
        },
        parse_dates=["transaction_date"],
        skip_blank_lines=True,
    )
    return df

def load_trades(data_dir: Path, year: int) -> pd.DataFrame:
    path = data_dir / f"{year}" / "transcation_record.csv"
    if not path.exists():
        raise FileNotFoundError(f"Trades file not found: {path}")

    df = pd.read_csv(
        path,
        dtype={
            "stock_symbol": "string",
            "side": "string",   # BUY/SELL
            "qty": "Int64",
            "price": "float64",
            "total_price": "float64",
        },
        parse_dates=["transaction_date"],
        skip_blank_lines=True,
    )
    return df

def load_actions(data_dir: Path, year: int) -> pd.DataFrame:
    path = data_dir / "actions.csv"
    if not path.exists():
        raise FileNotFoundError(f"Actions file not found: {path}")

    df = pd.read_csv(
        path,
        dtype={
            "symbol": "string",
            "action_type": "string",
            "ratio_from": "Int64",
            "ratio_to": "Int64",
        },
        parse_dates=["action_date"],
        skip_blank_lines=True,
    )
    return df

def prepare_action_queue(corp_actions_df: pd.DataFrame, year: int) -> Deque[dict]:

    if corp_actions_df is None or corp_actions_df.empty:
        return deque()

    df = corp_actions_df.copy()
    df = df.dropna(subset=['action_date', 'symbol', 'action_type', 'ratio_from', 'ratio_to']).copy()

    print(df)

    df["symbol"] = df["symbol"].astype("string")
    df["action_type"] = df["action_type"].astype("string")
    df["action_date"] = pd.to_datetime(df["action_date"])

    df = df[df["action_date"].dt.year == year].copy()
    df = df.sort_values(by=["action_date"]).reset_index(drop=True)

    return deque(df.to_dict("records"))

def apply_corporate_action(action: dict, inventories: Dict[str, Deque[Lot]]) -> None:
    symbol = str(action["symbol"]).strip()
    action_type = str(action["action_type"]).strip().upper()
    action_date = action.get("action_date")

    inventories.setdefault(symbol, deque())
    inv = inventories[symbol]

    if action_type == "SPLIT":
        rf = int(action["ratio_from"])
        rt = int(action["ratio_to"])

        if rf <= 0 or rt <= 0:
            raise ValueError(f"Invalid SPLIT ratio: {rf} -> {rt} for {symbol} on {action_date}")

        if rt % rf != 0:
            raise ValueError(f"SPLIT ratio must be integer multiple: {rf} -> {rt} for {symbol} on {action_date}")

        k = rt // rf

        for lot in inv:
            lot.qty = int(lot.qty) * k
            lot.price = float(lot.price) / k

    else:
        raise ValueError(f"Unsupported action_type: {action_type} ({symbol} {action_date})")


def inventory_df_to_queues(inventory_df: pd.DataFrame) -> Dict[str, Deque[Lot]]:
    
    inventories: Dict[str, Deque[Lot]] = defaultdict(deque)

    if inventory_df is None or inventory_df.empty:
        return inventories

    df = inventory_df.sort_values(by=["stock_symbol","transaction_date"]).copy()

    for row in df.itertuples(index=False):
        
        symbol = str(row.stock_symbol)
        qty = int(row.qty)
        price = float(row.price)
        date = row.transaction_date

        inventories[symbol].append(Lot(qty=qty, price=price, date=date))

    return inventories

def apply_trades_fifo(
    trades_df: pd.DataFrame,
    inventories: Dict[str, Deque[Lot]],
    actions_df: pd.DataFrame | None = None,
    year: int | None = None,
    # NEW: snapshot collector (date-driven)
    snapshot_collector: Optional[SnapshotCollector] = None,
) -> tuple[Dict[str, Deque[Lot]], pd.DataFrame, Optional[Dict[pd.Timestamp, Dict[str, int]]]]:

    # ---- prepare trades queue ----
    df = trades_df.copy()
    df = df.dropna(subset=["stock_symbol", "side", "qty", "price", "transaction_date"]).copy()
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    df["stock_symbol"] = df["stock_symbol"].astype("string").str.strip()
    df["side"] = df["side"].astype("string").str.strip().str.upper()
    df = df.sort_values(by=["transaction_date"]).reset_index(drop=True)
    trade_q: Deque[dict] = deque(df.to_dict("records"))

    # ---- prepare actions queue (only once) ----
    if actions_df is not None and year is not None:
        action_q: Deque[dict] = prepare_action_queue(actions_df, year)
    else:
        action_q = deque()

    realized_pnl_records: List[dict] = []

    def _peek_next_event_date() -> Optional[pd.Timestamp]:
        """Return the next event *date* (normalized) among trade/action queues."""
        td = trade_q[0]["transaction_date"].normalize() if trade_q else None
        ad = action_q[0]["action_date"].normalize() if action_q else None
        if td is None:
            return ad
        if ad is None:
            return td
        return td if td <= ad else ad

    # ---- merge loop ----
    # rule: same day -> trade first, then action
    while trade_q or action_q:
        next_event_date = _peek_next_event_date()

        # IMPORTANT: capture snapshots for any snapshot_date < next_event_date
        if snapshot_collector is not None:
            snapshot_collector.consume_until(next_event_date, inventories)

        td = trade_q[0]["transaction_date"] if trade_q else None
        ad = action_q[0]["action_date"] if action_q else None

        # action only runs if strictly earlier than next trade date
        # (same day trade first -> so NOT using <=)
        if ad is not None and (td is None or ad < td):
            act = action_q.popleft()
            apply_corporate_action(act, inventories)
            continue

        # otherwise process a trade
        tr = trade_q.popleft()
        symbol = str(tr["stock_symbol"]).strip()
        side = str(tr["side"]).upper()
        qty = int(tr["qty"])
        price = float(tr["price"])
        date = tr["transaction_date"]

        inventories.setdefault(symbol, deque())
        inventory = inventories[symbol]

        if side == "BUY":
            inventory.append(Lot(qty=qty, price=price, date=date))

        elif side == "SELL":
            remaining_qty = qty

            while remaining_qty > 0:
                if not inventory:
                    raise ValueError(f"Not enough inventory to sell for {symbol} on {date}")

                lot = inventory.popleft()
                lot_qty = int(lot.qty)
                lot_price = float(lot.price)
                lot_date = lot.date

                sell_qty = min(lot_qty, remaining_qty)
                remaining_qty -= sell_qty

                realized_pnl = round(sell_qty * (price - lot_price), 0)

                remaining_lot_qty = lot_qty - sell_qty
                if remaining_lot_qty > 0:
                    inventory.appendleft(Lot(qty=remaining_lot_qty, price=lot_price, date=lot_date))

                realized_pnl_records.append({
                    "transaction_date": date,
                    "stock_symbol": symbol,
                    "sell_qty": sell_qty,
                    "sell_price": price,
                    "buy_date": lot_date,
                    "buy_price": lot_price,
                    "realized_pnl": realized_pnl,
                })

        else:
            raise ValueError(f"Unknown trade side: {side} for {symbol} on {date}")

    # after finishing all events, capture remaining snapshot dates
    snapshots_out: Optional[Dict[pd.Timestamp, Dict[str, int]]] = None
    if snapshot_collector is not None:
        snapshot_collector.finalize(inventories)
        snapshots_out = snapshot_collector.snapshots

    return inventories, pd.DataFrame(realized_pnl_records), snapshots_out


def save_inventories(data_dir: Path, year:int,
                     inventories: Dict[str,Deque[Lot]]):
    path = data_dir / f"{year+1}" / "inventory.csv"
    path.parent.mkdir(parents=True, exist_ok=True)

    records = []

    for symbol, queue in inventories.items():
        for lot in queue:
            records.append({
                "transaction_date": lot.date,
                "stock_symbol": symbol,
                "qty": lot.qty,
                "price": lot.price,
            })

    inv_df = pd.DataFrame(
        records,
        columns=["transaction_date", "stock_symbol", "qty", "price"]
    )
    inv_df.to_csv(path, index=False)
    print(f"!!! Saved updated inventory to {path}")
    return path

def save_realized_pnl(
    data_dir: Path,
    year: int,
    realized_pnl_df: pd.DataFrame,
    ) -> Path:

    path = data_dir / f"{year}" / "realized_pnl.csv"
    path.parent.mkdir(parents=True, exist_ok=True)

    columns = [
        "transaction_date",
        "stock_symbol",
        "sell_qty",
        "sell_price",
        "buy_date",
        "buy_price",
        "realized_pnl",
    ]

    if realized_pnl_df is None or realized_pnl_df.empty:
        df = pd.DataFrame(columns=columns)
    else:
        df = realized_pnl_df.copy()
        df = df[columns]

    df.to_csv(path, index=False)
    return path


if __name__ == "__main__":
    data_dir = Path("data")
    year = 2025


    print('year:', year)

    inv_df = load_inventory(data_dir, year)
    trades_df = load_trades(data_dir, year)

    print("trades_df:"  , trades_df)

    inventories = inventory_df_to_queues(inv_df)
    print("inventories:",inventories)

    for symbol, queue in inventories.items():
        print(f"Inventory for {symbol}: {list(queue)}")

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