import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

os.environ.setdefault("SNAPSHOTS_DIR", tempfile.mkdtemp(prefix="drivers-scout-test-run-fetch-"))
os.environ.setdefault("IRACING_USERNAME", "user")
os.environ.setdefault("IRACING_PASSWORD", "pass")
os.environ.setdefault("IRACING_CLIENT_SECRET", "secret")
db_dir = Path(tempfile.mkdtemp(prefix="drivers-scout-test-run-fetch-db-"))
os.environ["DATABASE_URL"] = f"sqlite:///{db_dir / 'drivers-scout-test.db'}"

from app.api import public_router, router
from app.services import init_db
from app.settings import settings


class RunFetchEndpointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        settings.license_admin_secret = "letmein"
        settings.categories = "sports_car,formula_car"
        cls.db_path = Path("drivers-scout-test.db")
        if cls.db_path.exists():
            cls.db_path.unlink()
        init_db()
        cls.app = FastAPI()
        cls.app.include_router(public_router)
        cls.app.include_router(router)
        cls.client = TestClient(cls.app)
        cls.headers = {"X-Admin-Secret": "letmein"}

    def setUp(self) -> None:
        # Ensure categories are reset if tests mutate settings
        settings.categories = "sports_car,formula_car"

    @patch("app.api.fetch_and_store", new_callable=AsyncMock)
    def test_run_fetch_defaults_to_all_categories(self, mock_fetch: AsyncMock) -> None:
        mock_fetch.return_value = {"sports_car": 10, "formula_car": 5}

        response = self.client.post("/admin/run-fetch", headers=self.headers)

        self.assertEqual(response.status_code, 200)
        mock_fetch.assert_awaited_once_with(None)
        self.assertEqual(response.json()["counts"], mock_fetch.return_value)

    @patch("app.api.fetch_and_store", new_callable=AsyncMock)
    def test_run_fetch_accepts_supported_category(self, mock_fetch: AsyncMock) -> None:
        mock_fetch.return_value = {"formula_car": 5}

        response = self.client.post(
            "/admin/run-fetch",
            params={"category": "formula_car"},
            headers=self.headers,
        )

        self.assertEqual(response.status_code, 200)
        mock_fetch.assert_awaited_once_with("formula_car")
        self.assertEqual(response.json()["counts"], mock_fetch.return_value)

    @patch("app.api.fetch_and_store", new_callable=AsyncMock)
    def test_run_fetch_rejects_unsupported_category(self, mock_fetch: AsyncMock) -> None:
        response = self.client.post(
            "/admin/run-fetch",
            params={"category": "oval"},
            headers=self.headers,
        )

        self.assertEqual(response.status_code, 400)
        mock_fetch.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
