import os
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

os.environ.setdefault("SNAPSHOTS_DIR", tempfile.mkdtemp(prefix="drivers-scout-test-members-"))
os.environ.setdefault("IRACING_USERNAME", "user")
os.environ.setdefault("IRACING_PASSWORD", "pass")
os.environ.setdefault("IRACING_CLIENT_SECRET", "secret")
db_dir = Path(tempfile.mkdtemp(prefix="drivers-scout-test-members-db-"))
os.environ["DATABASE_URL"] = f"sqlite:///{db_dir / 'drivers-scout-test.db'}"

from app.api import router
from app.db import get_session
from app.models import Member
from app.services import init_db
from app.settings import settings


class MemberSearchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        settings.license_admin_secret = ""
        init_db()
        cls.app = FastAPI()
        cls.app.include_router(router)
        cls.client = TestClient(cls.app)

    def setUp(self) -> None:
        with get_session() as session:
            session.query(Member).delete()
            session.add_all(
                [
                    Member(cust_id=1, display_name="Alice Johnson", location="USA"),
                    Member(cust_id=2, display_name="Bob Smith", location="Canada"),
                    Member(cust_id=3, display_name="Alicia Keys", location="UK"),
                    Member(cust_id=4, display_name=None, location="Nowhere"),
                ]
            )

    def test_exact_match_returns_single_result(self) -> None:
        response = self.client.get("/members/search", params={"q": "Bob Smith"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["query"], "Bob Smith")
        self.assertEqual(payload["limit"], 20)
        self.assertEqual(payload["offset"], 0)
        self.assertEqual(len(payload["results"]), 1)
        self.assertEqual(payload["results"][0]["cust_id"], 2)
        self.assertEqual(payload["results"][0]["display_name"], "Bob Smith")
        self.assertEqual(payload["results"][0]["location"], "Canada")

    def test_partial_case_insensitive_match_returns_multiple(self) -> None:
        response = self.client.get("/members/search", params={"q": "ali", "limit": 5})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        returned_ids = {item["cust_id"] for item in payload["results"]}
        self.assertSetEqual(returned_ids, {1, 3})
        for item in payload["results"]:
            self.assertIsNotNone(item["display_name"])

    def test_returns_empty_results_for_unknown_query(self) -> None:
        response = self.client.get("/members/search", params={"q": "nomatch"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["results"], [])


if __name__ == "__main__":
    unittest.main()
