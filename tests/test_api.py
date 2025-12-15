import os
import shutil
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("SNAPSHOTS_DIR", tempfile.mkdtemp(prefix="drivers-scout-test-api-"))
os.environ.setdefault("IRACING_USERNAME", "user")
os.environ.setdefault("IRACING_PASSWORD", "pass")
os.environ.setdefault("IRACING_CLIENT_SECRET", "secret")
db_dir = Path(tempfile.mkdtemp(prefix="drivers-scout-test-api-db-"))
os.environ["DATABASE_URL"] = f"sqlite:///{db_dir / 'drivers-scout-test.db'}"

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import router
from app import services


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
        services._top_growers_cache.clear()
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
        self.assertEqual(results[0]["starts"], 20)
        self.assertEqual(results[0]["wins"], 2)

    def test_cache_reused_until_cutoff_then_refreshed(self) -> None:
        requested_days = 30
        early_now = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
        later_same_day = early_now + timedelta(hours=1)
        after_cutoff = early_now.replace(hour=23, minute=56)

        with patch("app.services._utcnow", return_value=early_now):
            response1 = self.client.get(
                "/leaders/growers",
                params={
                    "category": "sports_car",
                    "days": requested_days,
                    "limit": 5,
                },
            )
        self.assertEqual(response1.status_code, 200)
        payload1 = response1.json()
        self.assertEqual(payload1["start_date_used"], self.start_date.isoformat())

        with patch("app.services._utcnow", return_value=later_same_day), patch(
            "app.services.run_in_threadpool"
        ) as mock_threadpool:
            mock_threadpool.side_effect = RuntimeError("cache should be used")
            response2 = self.client.get(
                "/leaders/growers",
                params={
                    "category": "sports_car",
                    "days": requested_days,
                    "limit": 5,
                },
            )
        self.assertEqual(response2.status_code, 200)
        self.assertEqual(response1.json(), response2.json())

        with patch("app.services._utcnow", return_value=after_cutoff), patch(
            "app.services.run_in_threadpool", wraps=services.run_in_threadpool
        ) as mock_threadpool:
            response3 = self.client.get(
                "/leaders/growers",
                params={
                    "category": "sports_car",
                    "days": requested_days,
                    "limit": 5,
                },
            )
        self.assertEqual(response3.status_code, 200)
        self.assertTrue(mock_threadpool.called)


if __name__ == "__main__":
    unittest.main()
