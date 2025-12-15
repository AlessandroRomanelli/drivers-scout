import os
import shutil
import os
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

os.environ.setdefault("SNAPSHOTS_DIR", tempfile.mkdtemp(prefix="drivers-scout-test-api-"))
os.environ.setdefault("IRACING_USERNAME", "user")
os.environ.setdefault("IRACING_PASSWORD", "pass")
os.environ.setdefault("IRACING_CLIENT_SECRET", "secret")
db_dir = Path(tempfile.mkdtemp(prefix="drivers-scout-test-api-db-"))
os.environ["DATABASE_URL"] = f"sqlite:///{db_dir / 'drivers-scout-test.db'}"

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import router


class GrowersApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = FastAPI()
        cls.app.include_router(router)
        cls.client = TestClient(cls.app)
        cls.snapshots_dir = Path(os.environ["SNAPSHOTS_DIR"]) / "sports_car"

    def setUp(self) -> None:
        if self.snapshots_dir.exists():
            shutil.rmtree(self.snapshots_dir)
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)
        self.end_date = date.today()
        self.start_date = self.end_date - timedelta(days=10)
        self._write_csv(
            self.start_date,
            [
                ["CUSTID", "DRIVER", "LOCATION", "IRATING", "STARTS", "WINS"],
                ["1", "Driver One", "USA", "1500", "10", "1"],
                ["2", "Driver Two", "UK", "1200", "5", "0"],
            ],
        )
        self._write_csv(
            self.end_date,
            [
                ["CUSTID", "DRIVER", "LOCATION", "IRATING", "STARTS", "WINS"],
                ["1", "Driver One", "USA", "1700", "20", "2"],
                ["2", "Driver Two", "UK", "1250", "10", "1"],
            ],
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.snapshots_dir, ignore_errors=True)

    def _write_csv(self, snapshot_date: date, rows: list[list[str]]) -> None:
        path = self.snapshots_dir / f"{snapshot_date.isoformat()}.csv"
        content = "\n".join([",".join(row) for row in rows])
        path.write_text(content, encoding="utf-8")

    def test_leaders_endpoint_returns_growth(self) -> None:
        response = self.client.get(
            "/leaders/growers",
            params={"category": "sports_car", "days": 10, "limit": 5},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["category"], "sports_car")
        self.assertEqual(payload["snapshot_age_days"], (self.end_date - self.start_date).days)
        results = payload["results"]
        self.assertEqual([r["cust_id"] for r in results], [1, 2])
        self.assertEqual(results[0]["delta"], 200)
        self.assertEqual(results[0]["end_value"], 1700)


if __name__ == "__main__":
    unittest.main()
