"""Convert snapshot CSVs to pickle maps."""
from __future__ import annotations

import argparse
import logging
import pickle
from pathlib import Path
from typing import Dict, Iterator, Tuple
import csv
from datetime import date, datetime

SnapshotRow = Dict[str, object]

logger = logging.getLogger(__name__)

def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Extract typed fields from a CSV row with parsed values."""

    def parse_int(key: str) -> int | None:
        try:
            return int(row.get(key, ""))
        except (TypeError, ValueError):
            return None

    return {
        "cust_id": parse_int("CUSTID"),
        "display_name": row.get("DRIVER"),
        "location": row.get("LOCATION"),
        "irating": parse_int("IRATING"),
        "starts": parse_int("STARTS"),
        "wins": parse_int("WINS"),
    }

def load_snapshot_rows(path: Path) -> Iterator[SnapshotRow]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield normalize_row(row)

def parse_snapshot_date(path: Path) -> date | None:
    try:
        return datetime.strptime(path.stem, "%Y-%m-%d").date()
    except ValueError:
        logger.warning("Skipping snapshot with unexpected name: %s", path.name)
        return None
    
def load_snapshot_map(path: Path) -> Dict[int, SnapshotRow]:
    result: Dict[int, SnapshotRow] = {}
    for row in load_snapshot_rows(path):
        cust_id = row.get("cust_id")
        if isinstance(cust_id, int):
            result[cust_id] = row
    return result


def _iter_csv_paths(root: Path) -> list[Path]:
    if not root.exists():
        logger.error("Snapshot root does not exist: %s", root)
        return []
    return sorted(root.glob("*.csv"))


def _convert_path(path: Path, *, overwrite: bool) -> None:
    snapshot_date = parse_snapshot_date(path)
    if snapshot_date is None:
        return
    output_path = path.with_suffix(".pkl")
    if output_path.exists() and not overwrite:
        logger.info("Skipping existing snapshot map: %s", output_path)
        return
    try:
        snapshot_map = load_snapshot_map(path)
    except Exception:
        logger.exception("Failed to load snapshot CSV: %s", path)
        return
    try:
        output_path.write_bytes(
            pickle.dumps(snapshot_map, protocol=pickle.HIGHEST_PROTOCOL)
        )
        logger.info("Wrote snapshot map: %s", output_path)
    except Exception:
        logger.exception("Failed to write snapshot map: %s", output_path)


def run(root: Path, *, category: str | None, overwrite: bool) -> int:
    target_dir = root / category if category else root
    csv_paths = _iter_csv_paths(target_dir)
    if not csv_paths:
        logger.warning("No CSV snapshots found under %s", target_dir)
        return 1
    for path in csv_paths:
        _convert_path(path, overwrite=overwrite)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert snapshot CSVs to pickled maps.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        required=True,
        help="Root snapshots directory or category subdirectory.",
    )
    parser.add_argument(
        "--category",
        type=str,
        default=None,
        help="Optional category subdirectory under the root.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing .pkl files.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return run(args.root, category=args.category, overwrite=args.overwrite)


if __name__ == "__main__":
    raise SystemExit(main())
