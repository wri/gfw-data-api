import pytest
from alembic.config import main
from fastapi.testclient import TestClient
from app.routes import is_admin


async def is_admin_mocked():
    return True


@pytest.fixture
def client():
    """
    Set up a clean database before running a test
    Run all migrations before test and downgrade afterwards
    """
    from app.main import app
    from app.application import db

    main(["--raiseerr", "upgrade", "head"])
    app.dependency_overrides[is_admin] = is_admin_mocked

    with TestClient(app) as client:
        yield client

    app.dependency_overrides = {}
    main(["--raiseerr", "downgrade", "base"])
