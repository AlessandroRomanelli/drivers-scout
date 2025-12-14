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
db_dir = Path(tempfile.mkdtemp(prefix="drivers-scout-test-licenses-db-"))
os.environ["DATABASE_URL"] = f"sqlite:///{db_dir / 'drivers-scout-test.db'}"

from app.api import public_router, router
from app.db import get_session
from app.models import License
from app.services import init_db
from app.settings import settings


class LicenseAdminTests(unittest.TestCase):
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

    def test_admin_secret_required(self) -> None:
        response = self.client.post("/admin/licenses", json={"label": "alpha"})
        self.assertEqual(response.status_code, 401)

    def test_license_lifecycle(self) -> None:
        headers = {"X-Admin-Secret": "letmein"}
        created = self.client.post(
            "/admin/licenses", json={"label": "beta"}, headers=headers
        )
        self.assertEqual(created.status_code, 200)
        payload = created.json()
        self.assertEqual(len(payload["key"]), settings.license_key_length)
        self.assertTrue(payload["active"])

        listing = self.client.get("/admin/licenses", headers=headers)
        self.assertEqual(listing.status_code, 200)
        self.assertEqual(len(listing.json()), 1)

        revoke = self.client.post(
            f"/admin/licenses/{payload['key']}/revoke", headers=headers
        )
        self.assertEqual(revoke.status_code, 200)
        revoked_payload = revoke.json()
        self.assertFalse(revoked_payload["active"])
        self.assertIsNotNone(revoked_payload["revoked_at"])

        reactivate = self.client.post(
            f"/admin/licenses/{payload['key']}/activate", headers=headers
        )
        self.assertEqual(reactivate.status_code, 200)
        self.assertTrue(reactivate.json()["active"])


if __name__ == "__main__":
    unittest.main()
