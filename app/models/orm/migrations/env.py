"""env.py.

Alembic ENV module isort:skip_file
"""

# Native libraries
import sys

sys.path.extend(["./"])

######################## --- MODELS FOR MIGRATIONS --- ########################
from app.application import db

# To include a model in migrations, add a line here.
from app.models.orm.assets import Asset  # noqa: F401
from app.models.orm.datasets import Dataset  # noqa: F401
from app.models.orm.geostore import Geostore  # noqa: F401
from app.models.orm.tasks import Task  # noqa: F401
from app.models.orm.user_areas import UserArea  # noqa: F401
from app.models.orm.versions import Version  # noqa: F401
from app.models.orm.api_keys import ApiKey  # noqa: F401

###############################################################################

# Third party packages
from alembic import context
from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool


# App imports
from app.settings.globals import ALEMBIC_CONFIG


config = context.config
fileConfig(config.config_file_name)
target_metadata = db


def exclude_tables_from_config(config_):
    tables = None
    tables_ = config_.get("tables", None)
    if tables_ is not None:
        tables = tables_.split(",")
    return tables


exclude_tables = exclude_tables_from_config(config.get_section("alembic:exclude"))


def include_object(obj, name, type_, reflected, compare_to):
    if type_ == "table" and name in exclude_tables:
        return False
    else:
        return True


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.
    """
    context.configure(
        url=ALEMBIC_CONFIG.url.__to_string__(hide_password=False),
        target_metadata=target_metadata,
        literal_binds=True,
        include_object=include_object,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine and associate a
    connection with the context.
    """
    connectable = engine_from_config(
        {"sqlalchemy.url": ALEMBIC_CONFIG.url.__to_string__(hide_password=False)},
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
        )

        with context.begin_transaction() as transaction:
            context.run_migrations()
            if "dry-run" in context.get_x_argument():
                print("Dry-run succeeded; now rolling back transaction")
                transaction.rollback()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
