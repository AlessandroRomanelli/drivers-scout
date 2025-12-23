"""Convert snapshot CSVs to pickle maps."""
from __future__ import annotations

import argparse
import logging
import pickle
from pathlib import Path

from app import snapshots


logger = logging.getLogger(__name__)


def _iter_csv_paths(root: Path) -> list[Path]:
    if not root.exists():
        logger.error("Snapshot root does not exist: %s", root)
        return []
    return sorted(root.glob("*.csv"))


def _convert_path(path: Path, *, overwrite: bool) -> None:
    snapshot_date = snapshots.parse_snapshot_date(path)
    if snapshot_date is None:
        return
    output_path = path.with_suffix(".pkl")
    if output_path.exists() and not overwrite:
        logger.info("Skipping existing snapshot map: %s", output_path)
        return
    try:
        snapshot_map = snapshots.load_snapshot_map(path)
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
