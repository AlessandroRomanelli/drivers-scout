"""Utilities for storing and loading CSV snapshots on disk."""
from __future__ import annotations

import csv
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterator, Tuple

from .iracing_client import normalize_row
from .settings import settings

logger = logging.getLogger(__name__)


SnapshotRow = Dict[str, object]


def snapshot_directory(category: str) -> Path:
    return settings.snapshots_dir / category


def snapshot_path(category: str, snapshot_date: date) -> Path:
    directory = snapshot_directory(category)
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{snapshot_date.isoformat()}.csv"


def store_snapshot(category: str, snapshot_date: date, content: str) -> Path:
    path = snapshot_path(category, snapshot_date)
    path.write_text(content, encoding="utf-8")
    logger.info("Stored snapshot for %s at %s", category, path)
    return path


def list_snapshot_files(category: str) -> list[Path]:
    directory = snapshot_directory(category)
    if not directory.exists():
        return []
    return sorted(directory.glob("*.csv"))


def parse_snapshot_date(path: Path) -> date | None:
    try:
        return datetime.strptime(path.stem, "%Y-%m-%d").date()
    except ValueError:
        logger.warning("Skipping snapshot with unexpected name: %s", path.name)
        return None


def get_oldest_snapshot_date(category: str) -> date | None:
    oldest: date | None = None
    for path in list_snapshot_files(category):
        snapshot_date = parse_snapshot_date(path)
        if snapshot_date is None:
            continue
        if oldest is None or snapshot_date < oldest:
            oldest = snapshot_date
    return oldest


def find_closest_snapshot(category: str, target_date: date) -> Tuple[Path | None, date | None]:
    candidates: list[tuple[int, Path, date]] = []
    for path in list_snapshot_files(category):
        snapshot_date = parse_snapshot_date(path)
        if not snapshot_date:
            continue
        delta = abs((snapshot_date - target_date).days)
        candidates.append((delta, path, snapshot_date))
    if not candidates:
        return None, None
    candidates.sort(key=lambda item: (item[0], item[2]))
    _, path, snapshot_date = candidates[0]
    return path, snapshot_date


def load_snapshot_rows(path: Path) -> Iterator[SnapshotRow]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield normalize_row(row)


def load_snapshot_map(path: Path) -> Dict[int, SnapshotRow]:
    result: Dict[int, SnapshotRow] = {}
    for row in load_snapshot_rows(path):
        cust_id = row.get("cust_id")
        if isinstance(cust_id, int):
            result[cust_id] = row
    return result
