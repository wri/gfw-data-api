from typing import Optional

import pytest
from alembic.config import main
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

from app.routes import is_admin
from app.settings.globals import (
    WRITER_DBNAME,
    WRITER_PASSWORD,
    WRITER_USERNAME,
    WRITER_HOST,
    WRITER_PORT,
)

from sqlalchemy.orm import sessionmaker, Session

SessionLocal: Optional[Session] = None


async def is_admin_mocked():
    return True


@pytest.fixture
def client():
    """
    Set up a clean database before running a test
    Run all migrations before test and downgrade afterwards
    """
    from app.main import app

    main(["--raiseerr", "upgrade", "head"])
    app.dependency_overrides[is_admin] = is_admin_mocked

    with TestClient(app) as client:
        yield client

    app.dependency_overrides = {}
    main(["--raiseerr", "downgrade", "base"])


@pytest.fixture
def db():
    """
    Set up a clean database before running a test
    Run all migrations before test and downgrade afterwards
    """

    global SessionLocal

    if SessionLocal is None:
        db_conn = f"postgresql://{WRITER_USERNAME}:{WRITER_PASSWORD}@{WRITER_HOST}:{WRITER_PORT}/{WRITER_DBNAME}"  # pragma: allowlist secret
        engine = create_engine(db_conn, pool_size=1, max_overflow=0)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    db: Optional[Session] = None
    try:
        db = SessionLocal()
        yield db
    finally:
        if db is not None:
            db.close()
