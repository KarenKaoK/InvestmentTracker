import os
import pandas as pd
from pathlib import Path
from collections import deque, defaultdict
from typing import Dict, Deque, Tuple, List
from dataclasses import dataclass

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

def apply_trades_fifo(trades_df:pd.DataFrame,
                      inventories: Dict[str,Deque[Lot]],
                      ) -> pd.DataFrame:
    
    # trades_df process
    df = trades_df.copy()
    df = df.dropna(subset=['stock_symbol','side','qty','price','transaction_date']).copy()
    df = df.sort_values(by=["transaction_date"]).reset_index(drop=True)

    # create realized list 
    realized_pnl_records: List[dict] = []

    # one-line process each trade
    for _, row in df.iterrows():

        symbol = str(row.stock_symbol)
        side = str(row.side).upper().upper()
        qty = int(row.qty)
        price = float(row.price)
        date = row.transaction_date

        if symbol not in inventories:
            inventories[symbol] = deque()

        inventory = inventories[symbol]

        if side == "BUY":
            inventory.append(Lot(qty=qty, price=price, date=date,))

        elif side == "SELL":
            remaining_qty = qty

            while remaining_qty > 0:
                if not inventory:
                    raise ValueError(f"Not enough inventory to sell for {symbol} on {date}")

                lot = inventory.popleft()
                lot_qty, lot_price, lot_date = lot.qty, lot.price, lot.date

                if lot_qty <= remaining_qty:
                    sell_qty = lot_qty
                    realized_pnl = sell_qty * (price - lot_price)
                    remaining_qty -= sell_qty
                else:
                    sell_qty = remaining_qty
                    realized_pnl = sell_qty * (price - lot_price)
                    inventory.appendleft((lot_qty - sell_qty, lot_price, lot_date))
                    remaining_qty = 0

                realized_pnl_records.append({
                    "transaction_date": date,
                    "stock_symbol": symbol,
                    "sell_qty": sell_qty,
                    "sell_price": price,
                    "buy_date": lot.date,
                    "buy_price": lot.price,
                    "realized_pnl": realized_pnl,
                })
        else:
            raise ValueError(f"Unknown trade side: {side} for {symbol} on {date}")
    
    realized_pnl_df = pd.DataFrame(realized_pnl_records)
    return inventories, realized_pnl_df

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
    year = 2023

    print('year:', year)

    inv_df = load_inventory(data_dir, year)
    trades_df = load_trades(data_dir, year)

    print("trades_df:"  , trades_df)

    inventories = inventory_df_to_queues(inv_df)
    print("inventories:",inventories)

    for symbol, queue in inventories.items():
        print(f"Inventory for {symbol}: {list(queue)}")

    inventory, realized_pnl_df = apply_trades_fifo(trades_df, inventories)

    print("Updated Inventory:", inventory)
    print("Realized PnL DataFrame:")
    print(realized_pnl_df)

    save_inventories(data_dir, year, inventories)
    save_realized_pnl(data_dir, year, realized_pnl_df)