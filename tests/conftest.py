import pytest
from alembic.config import main
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """
    Set up a clean database before running a test
    Run all migrations before test and downgrade afterwards
    """
    from app.main import app
    from app.application import db

    main(["--raiseerr", "upgrade", "head"])

    with TestClient(app) as client:
        yield client

    main(["--raiseerr", "downgrade", "base"])
