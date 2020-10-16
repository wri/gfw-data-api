from asyncio import Future
from contextvars import ContextVar
from typing import Optional

from fastapi import FastAPI
from fastapi.logger import logger
from gino import create_engine
from gino_starlette import Gino, GinoEngine

from .settings.globals import (
    DATABASE_CONFIG,
    SQL_REQUEST_TIMEOUT,
    WRITE_DATABASE_CONFIG,
)

# Set the current engine using a ContextVar to assure
# that the correct connection is used during concurrent requests
CURRENT_ENGINE: ContextVar = ContextVar("engine")

WRITE_ENGINE: Optional[GinoEngine] = None
READ_ENGINE: Optional[GinoEngine] = None


class ContextualGino(Gino):
    """Override the Gino Metadata object to allow to dynamically change the
    binds."""

    @property
    def bind(self):
        try:
            e = CURRENT_ENGINE.get()
            bind = e.result()
            logger.debug(f"Set bind to {bind.repr(color=True)}")
            return bind
        except LookupError:
            # not in a request
            logger.debug("Not in a request, using default bind")
            return self._bind

    @bind.setter
    def bind(self, val):
        self._bind = val


app = FastAPI(title="GFW Data API", redoc_url="/")

# Create Contextual Database, using default connection and pool size = 0
# We will bind actual connection pools based on path operation using middleware
# This allows us to query load-balanced Aurora read replicas for read-only operations
# and Aurora Write Node for write operations
db = ContextualGino(
    app=app,
    host=DATABASE_CONFIG.host,
    port=DATABASE_CONFIG.port,
    user=DATABASE_CONFIG.username,
    password=DATABASE_CONFIG.password,
    database=DATABASE_CONFIG.database,
    pool_min_size=0,
)


class ContextEngine(object):
    def __init__(self, method):
        self.method = method

    async def __aenter__(self):
        """initialize objects."""
        try:
            e = CURRENT_ENGINE.get()
        except LookupError:
            e = Future()
            engine = await self.get_engine(self.method)
            e.set_result(engine)
            await e
        finally:
            self.token = CURRENT_ENGINE.set(e)

    async def __aexit__(self, _type, value, tb):
        """Uninitialize objects."""
        CURRENT_ENGINE.reset(self.token)

    @staticmethod
    async def get_engine(method: str) -> GinoEngine:
        """Select the database connection depending on request method."""
        if method.upper() == "WRITE":
            logger.debug("Use write engine")
            engine: GinoEngine = WRITE_ENGINE
        else:
            logger.debug("Use read engine")
            engine = READ_ENGINE
        return engine


@app.on_event("startup")
async def startup_event():
    """Initializing the database connections on startup."""

    global WRITE_ENGINE
    global READ_ENGINE

    WRITE_ENGINE = await create_engine(
        WRITE_DATABASE_CONFIG.url, max_size=5, min_size=1
    )
    logger.info(
        f"Database connection pool for write operation created: {WRITE_ENGINE.repr(color=True)}"
    )
    READ_ENGINE = await create_engine(
        DATABASE_CONFIG.url,
        max_size=10,
        min_size=5,
        command_timeout=SQL_REQUEST_TIMEOUT,
    )
    logger.info(
        f"Database connection pool for read operation created: {READ_ENGINE.repr(color=True)}"
    )


@app.on_event("shutdown")
async def shutdown_event():
    """Closing the database connections on shutdown."""
    global WRITE_ENGINE
    global READ_ENGINE

    if WRITE_ENGINE:
        logger.info(
            f"Closing database connection for write operations {WRITE_ENGINE.repr(color=True)}"
        )
        await WRITE_ENGINE.close()
        logger.info(
            f"Closed database connection for write operations {WRITE_ENGINE.repr(color=True)}"
        )
    if READ_ENGINE:
        logger.info(
            f"Closing database connection for read operations {READ_ENGINE.repr(color=True)}"
        )
        await READ_ENGINE.close()
        logger.info(
            f"Closed database connection for read operations {READ_ENGINE.repr(color=True)}"
        )
