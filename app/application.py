import logging
from asyncio import Future
from contextvars import ContextVar
from typing import Optional

from fastapi import FastAPI, Request
from gino import create_engine
from gino_starlette import Gino, GinoEngine


from .settings.globals import DATABASE_CONFIG, WRITE_DATABASE_CONFIG

# Set the current engine using a ContextVar to assure
# that the correct connection is used during concurrent requests
CURRENT_ENGINE: ContextVar = ContextVar("engine")

WRITE_ENGINE: Optional[GinoEngine] = None
READ_ENGINE: Optional[GinoEngine] = None


class ContextualGino(Gino):
    """
    Overide the Gino Metadata object to allow to dynamically change the binds
    """

    @property
    def bind(self):
        try:
            e = CURRENT_ENGINE.get()
            bind = e.result()
            logging.warning(f"Set bind to {bind.repr(color=True)}")
            return bind
        except LookupError:
            # not in a request
            logging.warning("Not in a request, using default bind")
            return self._bind

    @bind.setter
    def bind(self, val):
        self._bind = val


app = FastAPI()

# Create Contextual Database, using default connection and pool size = 0
# We will bind actual connection pools based on path operation using middleware
# This allows us to query load-balanced Aurora read replicas for read-only operations
# and Aurora Write Node for write operations
db = ContextualGino(
    app,
    host=DATABASE_CONFIG.host,
    port=DATABASE_CONFIG.port,
    user=DATABASE_CONFIG.username,
    password=DATABASE_CONFIG.password,
    database=DATABASE_CONFIG.database,
    pool_min_size=0,
)


async def get_engine(method: str) -> GinoEngine:
    """
    Select the database connection depending on request method
    """
    write_methods = ["PUT", "PATCH", "POST", "DELETE"]
    if method in write_methods:
        logging.warning("Use write engine")
        engine: GinoEngine = WRITE_ENGINE
    else:
        logging.warning("Use read engine")
        engine = READ_ENGINE
    return engine


@app.middleware("http")
async def set_db_mode(request: Request, call_next):
    """
    This middleware replaces the db engine depending on the request type.
    Read requests use the read only pool.
    Write requests use the write pool.
    """
    try:
        e = CURRENT_ENGINE.get()
    except LookupError:
        e = Future()
        engine = await get_engine(request.method)
        e.set_result(engine)
        await e
    finally:
        token = CURRENT_ENGINE.set(e)

    response = await call_next(request)
    CURRENT_ENGINE.reset(token)
    return response


@app.on_event("startup")
async def startup_event():
    """
    Initializing the database connections on startup
    """

    global WRITE_ENGINE
    global READ_ENGINE

    WRITE_ENGINE = await create_engine(
        WRITE_DATABASE_CONFIG.url, max_size=5, min_size=1
    )
    logging.warning(
        f"Database connection pool for write operation created: {WRITE_ENGINE.repr(color=True)}"
    )
    READ_ENGINE = await create_engine(DATABASE_CONFIG.url, max_size=10, min_size=5)
    logging.warning(
        f"Database connection pool for read operation created: {READ_ENGINE.repr(color=True)}"
    )


@app.on_event("shutdown")
async def shutdown_event():
    """
    Closing the database connections on shutdown
    """
    global WRITE_ENGINE
    global READ_ENGINE

    if WRITE_ENGINE:
        logging.warning(
            f"Closing database connection for write operations {WRITE_ENGINE.repr(color=True)}"
        )
        await WRITE_ENGINE.close()
        logging.warning(
            f"Closed database connection for write operations {WRITE_ENGINE.repr(color=True)}"
        )
    if READ_ENGINE:
        logging.warning(
            f"Closing database connection for read operations {READ_ENGINE.repr(color=True)}"
        )
        await READ_ENGINE.close()
        logging.warning(
            f"Closed database connection for read operations {READ_ENGINE.repr(color=True)}"
        )
