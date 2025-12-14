import os
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

os.environ.setdefault("SNAPSHOTS_DIR", tempfile.mkdtemp(prefix="drivers-scout-test-licenses-"))
os.environ.setdefault("IRACING_USERNAME", "user")
os.environ.setdefault("IRACING_PASSWORD", "pass")
os.environ.setdefault("IRACING_CLIENT_SECRET", "secret")
db_dir = Path(tempfile.mkdtemp(prefix="drivers-scout-test-licenses-status-db-"))
os.environ["DATABASE_URL"] = f"sqlite:///{db_dir / 'drivers-scout-test.db'}"

from app.api import public_router, router
from app.db import get_session
from app.models import License
from app.services import init_db
from app.settings import settings


class LicenseStatusTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        settings.license_admin_secret = "letmein"
        cls.db_path = Path("drivers-scout-test.db")
        if cls.db_path.exists():
            cls.db_path.unlink()
        init_db()
        cls.app = FastAPI()
        cls.app.include_router(public_router)
        cls.app.include_router(router)
        cls.client = TestClient(cls.app)

    def setUp(self) -> None:
        with get_session() as session:
            session.query(License).delete()

    def test_returns_invalid_for_unknown_license(self) -> None:
        response = self.client.get("/licenses/unknown/status")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["valid"])
        self.assertFalse(payload["active"])
        self.assertIsNone(payload["label"])
        self.assertIsNone(payload["revoked_at"])

    def test_reports_active_license(self) -> None:
        headers = {"X-Admin-Secret": "letmein"}
        created = self.client.post("/admin/licenses", json={"label": "gamma"}, headers=headers)
        license_key = created.json()["key"]

        status_response = self.client.get(f"/licenses/{license_key}/status")

        self.assertEqual(status_response.status_code, 200)
        payload = status_response.json()
        self.assertTrue(payload["valid"])
        self.assertTrue(payload["active"])
        self.assertEqual(payload["label"], "gamma")
        self.assertIsNone(payload["revoked_at"])

    def test_reports_revoked_license(self) -> None:
        headers = {"X-Admin-Secret": "letmein"}
        created = self.client.post("/admin/licenses", json={"label": "delta"}, headers=headers)
        license_key = created.json()["key"]
        self.client.post(f"/admin/licenses/{license_key}/revoke", headers=headers)

        status_response = self.client.get(f"/licenses/{license_key}/status")

        self.assertEqual(status_response.status_code, 200)
        payload = status_response.json()
        self.assertFalse(payload["valid"])
        self.assertFalse(payload["active"])
        self.assertEqual(payload["label"], "delta")
        self.assertIsNotNone(payload["revoked_at"])


if __name__ == "__main__":
    unittest.main()
