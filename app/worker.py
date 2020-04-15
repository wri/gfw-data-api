from .application import db
from .settings.globals import DATABASE_CONFIG


async def startup(ctx):
    """
    Binds a connection set to the db object.
    """
    await db.set_bind(DATABASE_CONFIG.url)


async def shutdown(ctx):
    """
    Pops the bind on the db object.
    """
    await db.pop_bind().close()


class WorkerSettings:
    """
    Settings for the ARQ worker.
    """

    on_startup = startup
    on_shutdown = shutdown

