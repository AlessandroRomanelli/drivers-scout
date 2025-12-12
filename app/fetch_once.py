"""CLI helper to fetch once."""
from __future__ import annotations

import asyncio

from .services import fetch_and_store, init_db


def main() -> None:
    init_db()
    asyncio.run(fetch_and_store())


if __name__ == "__main__":
    main()
