import logging

import pytest
from alembic.config import main
from starlette.config import environ
from fastapi.testclient import TestClient


environ["TESTING"] = "TRUE"


@pytest.fixture
def client():
    from app.main import app
    from app.application import db

    main(["--raiseerr", "upgrade", "head"])

    with TestClient(app) as client:
        yield client

    main(["--raiseerr", "downgrade", "base"])
