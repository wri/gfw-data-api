from contextlib import asynccontextmanager

import pytest
import pytest_asyncio
from alembic.config import main as migrate
from asgi_lifespan import LifespanManager
from fastapi import FastAPI


@pytest_asyncio.fixture
async def app():
    from app.main import app

    async with LifespanManager(app) as manager:
        print("We're in!")
        yield manager.app


@pytest.fixture(scope="module")
def db_ready():
    """make sure that the db is only initialized and torn down once per
    module."""
    migrate(["--raiseerr", "upgrade", "head"])
    yield

    migrate(["--raiseerr", "downgrade", "base"])
