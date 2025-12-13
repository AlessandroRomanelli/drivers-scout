import unittest
from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models import Base, Member, MemberStatsSnapshot
from app.repository import fetch_irating_deltas_for_category


class FetchIratingDeltasForCategoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:", future=True)
        Base.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    def tearDown(self) -> None:
        self.session.close()
        self.engine.dispose()

    def _add_member(self, cust_id: int, display_name: str = "Member") -> None:
        self.session.add(Member(cust_id=cust_id, display_name=display_name, location=None))

    def _add_snapshot(
        self,
        cust_id: int,
        snapshot_date: date,
        irating: int | None,
        *,
        category: str = "oval",
    ) -> None:
        self.session.add(
            MemberStatsSnapshot(
                cust_id=cust_id,
                category=category,
                snapshot_date=snapshot_date,
                fetched_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
                irating=irating,
                starts=0,
                wins=0,
            )
        )

    def test_returns_sorted_deltas_using_closest_snapshots(self) -> None:
        start_date = date(2024, 1, 10)
        end_date = date(2024, 1, 20)

        for cust_id in (1, 2, 3):
            self._add_member(cust_id)

        self._add_snapshot(1, date(2024, 1, 5), 1000)
        self._add_snapshot(1, date(2024, 1, 9), 1010)
        self._add_snapshot(1, date(2024, 1, 18), 1200)
        self._add_snapshot(1, date(2024, 1, 21), 1300)  # After end_date, should be ignored

        self._add_snapshot(2, date(2024, 1, 8), 800)
        self._add_snapshot(2, date(2024, 1, 15), 900)
        self._add_snapshot(2, date(2024, 1, 20), 950)

        # Member 3 has no snapshot on or before the start_date
        self._add_snapshot(3, date(2024, 1, 12), 700)
        self._add_snapshot(3, date(2024, 1, 19), 800)

        self.session.commit()

        deltas = fetch_irating_deltas_for_category(
            self.session,
            category="oval",
            start_date=start_date,
            end_date=end_date,
        )

        self.assertEqual([row["cust_id"] for row in deltas], [1, 2])
        self.assertEqual(deltas[0]["delta"], 190)
        self.assertEqual(deltas[1]["delta"], 150)
        self.assertEqual(deltas[0]["start_snapshot_date"], date(2024, 1, 9))
        self.assertEqual(deltas[0]["end_snapshot_date"], date(2024, 1, 18))
        self.assertEqual(deltas[1]["start_snapshot_date"], date(2024, 1, 8))
        self.assertEqual(deltas[1]["end_snapshot_date"], date(2024, 1, 20))

    def test_excludes_null_iratings_and_applies_limit(self) -> None:
        start_date = date(2024, 1, 10)
        end_date = date(2024, 1, 20)

        for cust_id in (1, 2, 3, 4):
            self._add_member(cust_id)

        # Missing start iRating
        self._add_snapshot(1, date(2024, 1, 5), None)
        self._add_snapshot(1, date(2024, 1, 18), 1000)

        # Missing end iRating
        self._add_snapshot(2, date(2024, 1, 5), 900)
        self._add_snapshot(2, date(2024, 1, 18), None)

        self._add_snapshot(3, date(2024, 1, 8), 700)
        self._add_snapshot(3, date(2024, 1, 19), 800)

        self._add_snapshot(4, date(2024, 1, 8), 1000)
        self._add_snapshot(4, date(2024, 1, 19), 1200)

        self.session.commit()

        deltas = fetch_irating_deltas_for_category(
            self.session,
            category="oval",
            start_date=start_date,
            end_date=end_date,
            limit=1,
        )

        self.assertEqual(len(deltas), 1)
        self.assertEqual(deltas[0]["cust_id"], 4)
        self.assertEqual(deltas[0]["delta"], 200)


if __name__ == "__main__":
    unittest.main()
