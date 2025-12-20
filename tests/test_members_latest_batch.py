import os
import shutil
import tempfile
import unittest
from datetime import date
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

os.environ.setdefault("SNAPSHOTS_DIR", tempfile.mkdtemp(prefix="drivers-scout-test-latest-"))
os.environ.setdefault("IRACING_USERNAME", "user")
os.environ.setdefault("IRACING_PASSWORD", "pass")
os.environ.setdefault("IRACING_CLIENT_SECRET", "secret")
db_dir = Path(tempfile.mkdtemp(prefix="drivers-scout-test-latest-db-"))
os.environ["DATABASE_URL"] = f"sqlite:///{db_dir / 'drivers-scout-test.db'}"

from app.api import router
from app.settings import settings


class LatestMembersBatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        settings.license_admin_secret = ""
        cls.app = FastAPI()
        cls.app.include_router(router)
        cls.client = TestClient(cls.app)
        cls.snapshots_dir = Path(os.environ["SNAPSHOTS_DIR"]) / "sports_car"

    def setUp(self) -> None:
        if self.snapshots_dir.exists():
            shutil.rmtree(self.snapshots_dir)
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)
        self.snapshot_date = date.today()
        self._write_csv(
            self.snapshot_date,
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

    def test_latest_members_returns_results(self) -> None:
        response = self.client.get("/members/latest", params={"cust_ids": "1,2"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["category"], "sports_car")
        self.assertEqual(payload["snapshot_date"], self.snapshot_date.isoformat())
        self.assertEqual(payload["missing"], [])
        self.assertEqual([item["cust_id"] for item in payload["results"]], [1, 2])

    def test_latest_members_returns_missing_ids(self) -> None:
        response = self.client.get("/members/latest", params={"cust_ids": "1,999"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual([item["cust_id"] for item in payload["results"]], [1])
        self.assertEqual(payload["missing"], [999])

    def test_latest_members_rejects_invalid_cust_ids(self) -> None:
        response = self.client.get("/members/latest", params={"cust_ids": "1,abc"})

        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid cust_id", response.json()["detail"])

    def test_latest_members_rejects_invalid_category(self) -> None:
        response = self.client.get(
            "/members/latest",
            params={"cust_ids": "1,2", "category": "unknown"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "Unsupported category")


if __name__ == "__main__":
    unittest.main()
