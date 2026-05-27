import os
import pickle
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("SNAPSHOTS_DIR", tempfile.mkdtemp(prefix="drivers-scout-test-cache-"))
os.environ.setdefault("IRACING_USERNAME", "user")
os.environ.setdefault("IRACING_PASSWORD", "pass")
os.environ.setdefault("IRACING_CLIENT_SECRET", "secret")
from app import snapshots  # noqa: E402  -- env must be set before import


def _write_pickle(path: Path, payload: dict) -> Path:
    path.write_bytes(pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL))
    return path


class SnapshotPickleCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp(prefix="drivers-scout-pickle-"))
        # Reset the LRU between tests so the (path, mtime) keyspace is clean.
        snapshots._load_snapshot_map_binary.cache_clear()

    def _write(self, name: str, payload: dict) -> Path:
        return _write_pickle(self.tmpdir / f"{name}.pkl", payload)

    def test_hit_on_second_call_with_unchanged_file(self) -> None:
        p = self._write("a", {1: {"cust_id": 1, "display_name": "Driver A"}})
        mtime = p.stat().st_mtime

        first = snapshots._load_snapshot_map_binary(str(p), mtime)
        second = snapshots._load_snapshot_map_binary(str(p), mtime)

        # Same object identity → served from cache, no second disk read.
        self.assertIs(first, second)
        info = snapshots._load_snapshot_map_binary.cache_info()
        self.assertEqual(info.hits, 1)
        self.assertEqual(info.misses, 1)

    def test_invalidates_when_mtime_changes(self) -> None:
        p = self._write("a", {1: {"cust_id": 1, "display_name": "Driver A v1"}})
        mtime_v1 = p.stat().st_mtime

        v1 = snapshots._load_snapshot_map_binary(str(p), mtime_v1)
        self.assertEqual(v1[1]["display_name"], "Driver A v1")

        # Bump mtime by overwriting the file with new content + setting a later mtime.
        p.write_bytes(pickle.dumps({1: {"cust_id": 1, "display_name": "Driver A v2"}}))
        new_mtime = mtime_v1 + 10.0
        os.utime(p, (new_mtime, new_mtime))

        v2 = snapshots._load_snapshot_map_binary(str(p), new_mtime)

        # Different cache key → fresh load returns new contents.
        self.assertEqual(v2[1]["display_name"], "Driver A v2")
        self.assertIsNot(v1, v2)

    def test_evicts_oldest_when_capacity_exceeded(self) -> None:
        # Size-agnostic: derive maxsize from the live cache so the test passes
        # regardless of which env value the process booted with. Write maxsize+1
        # distinct paths and verify the first one was evicted.
        maxsize = snapshots._load_snapshot_map_binary.cache_info().maxsize
        self.assertGreaterEqual(maxsize, 1)

        paths = [
            self._write(f"p{i}", {i: {"cust_id": i}}) for i in range(maxsize + 1)
        ]
        for p in paths:
            snapshots._load_snapshot_map_binary(str(p), p.stat().st_mtime)

        info = snapshots._load_snapshot_map_binary.cache_info()
        self.assertEqual(info.currsize, maxsize)
        self.assertEqual(info.misses, maxsize + 1)

        # Re-load the first path → must miss again (it was the LRU and was evicted).
        snapshots._load_snapshot_map_binary(str(paths[0]), paths[0].stat().st_mtime)
        self.assertEqual(snapshots._load_snapshot_map_binary.cache_info().misses, maxsize + 2)

    def test_load_snapshot_map_cached_falls_back_to_csv_on_pickle_error(self) -> None:
        csv_path = self.tmpdir / "broken.csv"
        csv_path.write_text(
            "CUSTID,DRIVER,LOCATION,IRATING,STARTS,WINS\n7,CSV Driver,US,1500,1,0\n",
            encoding="utf-8",
        )
        pkl_path = csv_path.with_suffix(".pkl")
        pkl_path.write_bytes(b"not a real pickle blob")

        result = snapshots.load_snapshot_map_cached(csv_path)

        self.assertEqual(result[7]["display_name"], "CSV Driver")


if __name__ == "__main__":
    unittest.main()
