"""Utilities for storing and loading CSV snapshots on disk."""
from __future__ import annotations

import csv
import io
import logging
import pickle
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


def snapshot_map_path(category: str, snapshot_date: date) -> Path:
    directory = snapshot_directory(category)
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{snapshot_date.isoformat()}.pkl"


def _snapshot_map_from_content(content: str) -> Dict[int, SnapshotRow]:
    result: Dict[int, SnapshotRow] = {}
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        normalized = normalize_row(row)
        cust_id = normalized.get("cust_id")
        if isinstance(cust_id, int):
            result[cust_id] = normalized
    return result


def store_snapshot_map(
    category: str, snapshot_date: date, snapshot_map: Dict[int, SnapshotRow]
) -> Path:
    path = snapshot_map_path(category, snapshot_date)
    path.write_bytes(pickle.dumps(snapshot_map, protocol=pickle.HIGHEST_PROTOCOL))
    logger.info("Stored snapshot map for %s at %s", category, path)
    return path


def store_snapshot(
    category: str,
    snapshot_date: date,
    content: str,
    *,
    emit_map: bool = True,
) -> Path:
    path = snapshot_path(category, snapshot_date)
    path.write_text(content, encoding="utf-8")
    if emit_map:
        try:
            snapshot_map = _snapshot_map_from_content(content)
            store_snapshot_map(category, snapshot_date, snapshot_map)
        except Exception:
            logger.exception(
                "Failed to store snapshot map for %s on %s",
                category,
                snapshot_date,
            )
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


def _load_snapshot_map_binary(path: str, mtime: float) -> Dict[int, SnapshotRow]:
    with Path(path).open("rb") as handle:
        return pickle.load(handle)


def load_snapshot_map_cached(path: Path) -> Dict[int, SnapshotRow]:
    binary_path = path.with_suffix(".pkl")  
    if binary_path.exists():
        try:
            return _load_snapshot_map_binary(
                str(binary_path), binary_path.stat().st_mtime
            )
        except Exception:
            logger.exception(
                "Failed to load snapshot map from %s; falling back to CSV",
                binary_path,
            )
    return load_snapshot_map(path)
