import os
import shutil
import tempfile
import unittest
from datetime import date
from pathlib import Path

os.environ.setdefault("SNAPSHOTS_DIR", tempfile.mkdtemp(prefix="drivers-scout-test-member-sync-"))
os.environ.setdefault("IRACING_USERNAME", "user")
os.environ.setdefault("IRACING_PASSWORD", "pass")
os.environ.setdefault("IRACING_CLIENT_SECRET", "secret")
db_dir = Path(tempfile.mkdtemp(prefix="drivers-scout-test-member-sync-db-"))
os.environ["DATABASE_URL"] = f"sqlite:///{db_dir / 'drivers-scout-test.db'}"

from app.db import get_session
from app.models import Member
from app.services import init_db, sync_members_from_snapshots
from app.settings import settings


class MemberSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        self.snapshots_root = Path(os.environ["SNAPSHOTS_DIR"])
        shutil.rmtree(self.snapshots_root, ignore_errors=True)
        self.snapshots_root.mkdir(parents=True, exist_ok=True)

        db_path = Path(os.environ["DATABASE_URL"].split("///")[-1])
        if db_path.exists():
            db_path.unlink()
        init_db()

        settings.categories = "sports_car,formula_car"
        self._write_snapshot("sports_car", date(2024, 1, 1), [
            ["CUSTID", "DRIVER", "LOCATION", "IRATING", "STARTS", "WINS"],
            ["1", "Driver One", "USA", "1000", "0", "0"],
            ["2", "Driver Two", "UK", "1100", "0", "0"],
        ])
        self._write_snapshot("formula_car", date(2024, 1, 2), [
            ["CUSTID", "DRIVER", "LOCATION", "IRATING", "STARTS", "WINS"],
            ["1", "Driver One", "", "1200", "0", "0"],
            ["3", "Driver Three", "AUS", "1050", "0", "0"],
        ])

    def _write_snapshot(self, category: str, snapshot_date: date, rows: list[list[str]]) -> None:
        category_dir = self.snapshots_root / category
        category_dir.mkdir(parents=True, exist_ok=True)
        path = category_dir / f"{snapshot_date.isoformat()}.csv"
        content = "\n".join([",".join(row) for row in rows])
        path.write_text(content, encoding="utf-8")

    def test_sync_members_from_latest_snapshots(self) -> None:
        counts = sync_members_from_snapshots()
        self.assertEqual(counts, 3)

        with get_session() as session:
            members = session.query(Member).all()
            self.assertEqual(len(members), 3)
            by_id = {member.cust_id: member for member in members}
            self.assertEqual(by_id[1].display_name, "Driver One")
            self.assertEqual(by_id[1].location, "USA")

        # Run sync again to ensure no duplicate inserts occur
        counts_second_run = sync_members_from_snapshots()
        self.assertEqual(counts_second_run, 3)

        with get_session() as session:
            members = session.query(Member).all()
            self.assertEqual(len(members), 3)


if __name__ == "__main__":
    unittest.main()
