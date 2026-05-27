import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("SNAPSHOTS_DIR", tempfile.mkdtemp(prefix="drivers-scout-test-lookup-"))
os.environ.setdefault("IRACING_USERNAME", "user")
os.environ.setdefault("IRACING_PASSWORD", "pass")
os.environ.setdefault("IRACING_CLIENT_SECRET", "secret")
db_dir = Path(tempfile.mkdtemp(prefix="drivers-scout-test-lookup-db-"))
os.environ["DATABASE_URL"] = f"sqlite:///{db_dir / 'drivers-scout-test.db'}"

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import router
from app.db import get_session
from app.models import License, Member
from app.services import init_db
from app.settings import settings


class MembersLookupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        init_db()
        cls.app = FastAPI()
        cls.app.include_router(router)
        cls.client = TestClient(cls.app)

    def setUp(self) -> None:
        settings.license_admin_secret = ""
        with get_session() as session:
            session.query(Member).delete()
            session.query(License).delete()
            session.add_all(
                [
                    Member(
                        cust_id=10,
                        display_name="Lukas Lindqvist",
                        location="FI",
                        display_name_folded="lukas lindqvist",
                    ),
                    Member(
                        cust_id=11,
                        display_name="Müller",
                        location="DE",
                        display_name_folded="muller",
                    ),
                    Member(
                        cust_id=12,
                        display_name="Pablo H Santos",
                        location="ES",
                        display_name_folded="pablo h santos",
                    ),
                    # display_name is unique in iRacing's published data, so
                    # exact-match ambiguity isn't realistic. Folded ambiguity
                    # IS — "Sántos" and "Santos" both fold to "santos".
                    Member(
                        cust_id=13,
                        display_name="Carlos Sántos",
                        location="X",
                        display_name_folded="carlos santos",
                    ),
                    Member(
                        cust_id=14,
                        display_name="Carlos Santos",
                        location="Y",
                        display_name_folded="carlos santos",
                    ),
                ]
            )

    def test_empty_names_rejected(self) -> None:
        response = self.client.post("/members/lookup", json={"names": []})
        self.assertEqual(response.status_code, 422)

    def test_over_500_names_rejected(self) -> None:
        response = self.client.post(
            "/members/lookup", json={"names": ["x"] * 501}
        )
        self.assertEqual(response.status_code, 422)

    def test_empty_string_in_names_rejected(self) -> None:
        response = self.client.post("/members/lookup", json={"names": ["   "]})
        self.assertEqual(response.status_code, 422)

    def test_exact_match_resolution(self) -> None:
        response = self.client.post(
            "/members/lookup", json={"names": ["Lukas Lindqvist"]}
        )
        self.assertEqual(response.status_code, 200)
        resolutions = response.json()["resolutions"]
        self.assertEqual(len(resolutions), 1)
        self.assertEqual(resolutions[0]["query"], "Lukas Lindqvist")
        self.assertEqual(resolutions[0]["match_type"], "exact")
        self.assertEqual(resolutions[0]["cust_id"], 10)
        self.assertEqual(resolutions[0]["location"], "FI")

    def test_exact_match_is_case_and_whitespace_insensitive(self) -> None:
        response = self.client.post(
            "/members/lookup", json={"names": ["  lukas lindqvist  "]}
        )
        self.assertEqual(response.status_code, 200)
        res = response.json()["resolutions"][0]
        self.assertEqual(res["match_type"], "exact")
        self.assertEqual(res["cust_id"], 10)

    def test_folded_match_resolution(self) -> None:
        response = self.client.post(
            "/members/lookup", json={"names": ["Muller"]}
        )
        self.assertEqual(response.status_code, 200)
        res = response.json()["resolutions"][0]
        self.assertEqual(res["match_type"], "folded")
        self.assertEqual(res["cust_id"], 11)
        self.assertEqual(res["display_name"], "Müller")

    def test_folded_ambiguity_returns_null(self) -> None:
        # "Carlos Santos" (no accent) and "Carlos Sántos" both fold to
        # "carlos santos". A search for "Carlos Santos" exactly matches one
        # row (cust_id=14), so the exact path wins. A search for an unaccented
        # spelling that doesn't exist exactly — e.g. "Cárlos Santos" — folds
        # to "carlos santos" and matches both rows → ambiguous → null.
        response = self.client.post(
            "/members/lookup", json={"names": ["Cárlos Sántos"]}
        )
        self.assertEqual(response.status_code, 200)
        res = response.json()["resolutions"][0]
        self.assertIsNone(res["match_type"])
        self.assertIsNone(res["cust_id"])

    def test_exact_match_wins_over_folded_collision(self) -> None:
        # "Carlos Santos" matches cust_id=14 exactly; the folded collision
        # with "Carlos Sántos" must NOT mask that.
        response = self.client.post(
            "/members/lookup", json={"names": ["Carlos Santos"]}
        )
        self.assertEqual(response.status_code, 200)
        res = response.json()["resolutions"][0]
        self.assertEqual(res["match_type"], "exact")
        self.assertEqual(res["cust_id"], 14)

    def test_unknown_name_returns_null(self) -> None:
        response = self.client.post(
            "/members/lookup", json={"names": ["Totally Unknown"]}
        )
        self.assertEqual(response.status_code, 200)
        res = response.json()["resolutions"][0]
        self.assertIsNone(res["match_type"])

    def test_mixed_resolved_and_unresolved_preserves_order(self) -> None:
        names = [
            "Lukas Lindqvist",
            "Totally Unknown",
            "Muller",
            "Cárlos Sántos",  # folded ambiguity → null
            "Pablo H Santos",
        ]
        response = self.client.post("/members/lookup", json={"names": names})
        self.assertEqual(response.status_code, 200)
        resolutions = response.json()["resolutions"]
        self.assertEqual([r["query"] for r in resolutions], names)
        self.assertEqual(
            [r["match_type"] for r in resolutions],
            ["exact", None, "folded", None, "exact"],
        )
        self.assertEqual(
            [r["cust_id"] for r in resolutions],
            [10, None, 11, None, 12],
        )

    def test_unsupported_category_rejected(self) -> None:
        response = self.client.post(
            "/members/lookup",
            json={"names": ["Lukas Lindqvist"], "category": "rallycross"},
        )
        self.assertEqual(response.status_code, 400)

    def test_license_gated_when_admin_secret_set(self) -> None:
        settings.license_admin_secret = "any-secret"
        try:
            with get_session() as session:
                session.add(License(key="valid-key", label="test", active=True))

            unauth = self.client.post(
                "/members/lookup", json={"names": ["Lukas Lindqvist"]}
            )
            self.assertEqual(unauth.status_code, 401)

            auth = self.client.post(
                "/members/lookup",
                json={"names": ["Lukas Lindqvist"]},
                headers={"X-License-Key": "valid-key"},
            )
            self.assertEqual(auth.status_code, 200)
        finally:
            settings.license_admin_secret = ""


if __name__ == "__main__":
    unittest.main()
