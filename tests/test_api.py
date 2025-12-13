import os
import tempfile
import unittest
from datetime import datetime, date, timezone
from pathlib import Path

# Ensure configuration values exist before importing application modules
_temp_dir = tempfile.mkdtemp(prefix="drivers-scout-test-")
os.environ.setdefault("DATABASE_URL", f"sqlite:///{Path(_temp_dir) / 'api.db'}")
os.environ.setdefault("IRACING_USERNAME", "user")
os.environ.setdefault("IRACING_PASSWORD", "pass")
os.environ.setdefault("IRACING_CLIENT_SECRET", "secret")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import router
from app.db import SessionLocal, engine
from app.models import Base, Member, MemberStatsSnapshot


class MemberSnapshotApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = FastAPI()
        cls.app.include_router(router)
        cls.client = TestClient(cls.app)

    def setUp(self) -> None:
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)
        self.session = SessionLocal()
        member = Member(cust_id=123, display_name="Speedy Racer", location="USA")
        snapshot = MemberStatsSnapshot(
            cust_id=123,
            category="sports_car",
            snapshot_date=date(2024, 7, 1),
            fetched_at=datetime(2024, 7, 1, tzinfo=timezone.utc),
            irating=2500,
            starts=10,
            wins=2,
        )
        self.session.add_all([member, snapshot])
        self.session.commit()

    def tearDown(self) -> None:
        self.session.close()

    def test_latest_member_snapshot_includes_member_details(self) -> None:
        response = self.client.get("/members/123/latest", params={"category": "sports_car"})
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["driver"], "Speedy Racer")
        self.assertEqual(data["location"], "USA")
        self.assertEqual(data["irating"], 2500)

    def test_history_includes_member_details(self) -> None:
        response = self.client.get("/members/123/history", params={"category": "sports_car"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["driver"], "Speedy Racer")
        self.assertEqual(payload[0]["location"], "USA")
        self.assertEqual(payload[0]["starts"], 10)


if __name__ == "__main__":
    unittest.main()
