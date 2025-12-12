"""FastAPI application entry point."""
from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from .api import router
from .scheduler import shutdown_scheduler, start_scheduler
from .services import init_db
from .settings import settings

log_file_path = settings.log_file
log_file_parent = log_file_path.parent
if not log_file_parent.exists():
    log_file_parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file_path),
    ],
    force=True,
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing database")
    init_db()
    start_scheduler()
    try:
        yield
    finally:
        shutdown_scheduler()


app = FastAPI(title="Drivers Scout", lifespan=lifespan)
app.include_router(router)


def main() -> None:
    """Run the ASGI server."""
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        reload=False,
    )


if __name__ == "__main__":
    main()
