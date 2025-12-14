import os
import asyncio
import os
import shutil
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

os.environ.setdefault("SNAPSHOTS_DIR", tempfile.mkdtemp(prefix="drivers-scout-test-services-"))
os.environ.setdefault("IRACING_USERNAME", "user")
os.environ.setdefault("IRACING_PASSWORD", "pass")
os.environ.setdefault("IRACING_CLIENT_SECRET", "secret")
db_dir = Path(tempfile.mkdtemp(prefix="drivers-scout-test-services-db-"))
os.environ["DATABASE_URL"] = f"sqlite:///{db_dir / 'drivers-scout-test.db'}"

from app.services import get_irating_delta, get_top_growers


class SnapshotComputationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.category = "sports_car"
        self.snapshots_dir = Path(os.environ["SNAPSHOTS_DIR"]) / self.category
        if self.snapshots_dir.exists():
            shutil.rmtree(self.snapshots_dir)
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)
        self.end_date = date.today()
        self.start_date = self.end_date - timedelta(days=5)
        self._write_csv(
            self.start_date,
            [
                ["CUSTID", "DRIVER", "LOCATION", "IRATING", "STARTS", "WINS"],
                ["10", "Starter", "USA", "-1", "0", "0"],
                ["11", "Grower", "UK", "900", "0", "0"],
            ],
        )
        self._write_csv(
            self.end_date,
            [
                ["CUSTID", "DRIVER", "LOCATION", "IRATING", "STARTS", "WINS"],
                ["10", "Starter", "USA", "1500", "0", "0"],
                ["11", "Grower", "UK", "1200", "0", "0"],
            ],
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.snapshots_dir, ignore_errors=True)

    def _write_csv(self, snapshot_date: date, rows: list[list[str]]) -> None:
        path = self.snapshots_dir / f"{snapshot_date.isoformat()}.csv"
        content = "\n".join([",".join(row) for row in rows])
        path.write_text(content, encoding="utf-8")

    def test_top_growers_respects_min_irating_and_normalizes_start(self) -> None:
        data = asyncio.run(
            get_top_growers(self.category, days=5, limit=5, min_current_irating=1000)
        )
        self.assertEqual(data["start_date_used"], self.start_date)
        self.assertEqual(data["end_date_used"], self.end_date)
        self.assertEqual(data["snapshot_age_days"], (self.end_date - self.start_date).days)
        results = data["results"]
        self.assertEqual([r["cust_id"] for r in results], [11, 10])
        self.assertEqual(results[0]["delta"], 300)
        self.assertEqual(results[1]["start_value"], 1500)

    def test_irating_delta_returns_none_without_data(self) -> None:
        missing_date = self.end_date - timedelta(days=60)
        result = asyncio.run(
            get_irating_delta(99, self.category, start_date=missing_date, end_date=self.end_date)
        )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
