# src/snapshots.py
from __future__ import annotations

from typing import Dict, Deque, Tuple, Set, List, Optional, Protocol
import pandas as pd


class HasQty(Protocol):
    qty: int


Inventories = Dict[str, Deque[HasQty]]


class SnapshotCollector:

    def __init__(self, snapshot_dates: List[pd.Timestamp] | None):
        snapshot_dates = snapshot_dates or []

        # normalize & sort
        self._dates: List[pd.Timestamp] = sorted(
            pd.to_datetime(d).normalize() for d in snapshot_dates
        )
        self._i: int = 0  # pointer

        self.snapshots: Dict[pd.Timestamp, Dict[str, int]] = {}

    def consume_until(
        self,
        next_event_date: Optional[pd.Timestamp],
        inventories: Inventories,
    ) -> None:
        """
        Capture snapshots for all snapshot_date < next_event_date.

        If next_event_date is None:
          capture all remaining snapshot dates.
        """
        cutoff = None if next_event_date is None else pd.to_datetime(next_event_date).normalize()

        while self._i < len(self._dates):
            snap_date = self._dates[self._i]

            # not crossed yet
            if cutoff is not None and snap_date >= cutoff:
                break

            # capture full inventory state
            self.snapshots[snap_date] = self._snapshot_inventory(inventories)

            self._i += 1

    def finalize(self, inventories: Inventories) -> None:
        """Capture all remaining snapshot dates after the last event."""
        self.consume_until(None, inventories)

    @staticmethod
    def _snapshot_inventory(inventories: Inventories) -> Dict[str, int]:
        """
        Convert inventories to:
          symbol -> total_qty
        """
        snap: Dict[str, int] = {}
        for symbol, lots in inventories.items():
            total = sum(int(lot.qty) for lot in lots)
            if total != 0:
                snap[symbol] = int(total)
        return snap


if __name__ == "__main__":
    from collections import deque

    print("=== SnapshotCollector manual unit test ===")

    # --- 1. snapshot dates ---
    snapshot_dates = [
        pd.Timestamp("2023-01-29"),
        pd.Timestamp("2023-06-14"),
        pd.Timestamp("2023-09-30"),
    ]

    collector = SnapshotCollector(snapshot_dates)

    # --- 2. fake inventories ---
    class Lot:
        def __init__(self, qty: int):
            self.qty = qty

        def __repr__(self):
            return f"Lot(qty={self.qty})"

    inventories: Inventories = {
        "2330": deque(),
        "0050": deque(),
    }

    # --- 3. before first snapshot ---
    print('買賣操作')
    inventories["2330"].append(Lot(100))
    inventories["0050"].append(Lot(50))

    
    print('買賣操作實際庫存', inventories)

    # crossing 2023-01-29
    collector.consume_until(pd.Timestamp("2023-02-01"), inventories)
    print('快照：',collector.snapshots)

    
    print('第二次買賣操作')
    
    # --- 4. inventory changes ---
    # simulate sell
    inventories["2330"].append(Lot(-20))   # total = 80
    inventories["0050"].append(Lot(30))    # total = 80

    print('買賣操作實際庫存', inventories)

    # crossing 2023-06-14
    collector.consume_until(pd.Timestamp("2023-06-20"), inventories)

    print('\n快照：')
    print(collector.snapshots)

    # --- 5. finalize ---
    collector.finalize(inventories)

    print("剩餘的快照")
    print(collector.snapshots)

    print("\n=== Test finished ===")
