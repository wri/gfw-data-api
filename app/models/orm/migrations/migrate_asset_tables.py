"""Add version column to all data tables."""
import os

import click
import sqlalchemy as sa


def get_datasets(connection):
    datasets = connection.execute(sa.text("select * from public.versions"))
    return datasets.fetchall()


def upgrade(connection):
    versions = get_datasets(connection)
    for version in versions:
        if not connection.engine.has_table(version.version, schema=version.dataset):
            continue
        print("FOUND TABLE", version.version, version.dataset)
        connection.execute(
            sa.text(
                f"""ALTER TABLE "{version.dataset}"."{version.version}"
                ADD COLUMN "version" VARCHAR
            """
            )
        )
        connection.execute(
            sa.text(
                f"""UPDATE "{version.dataset}"."{version.version}" SET version = '{version.version}'
            """
            )
        )
        connection.execute(
            sa.text(
                f"""ALTER TABLE "{version.dataset}"."{version.version}"
                ALTER version SET NOT NULL
            """
            )
        )


def downgrade(connection):
    versions = get_datasets(connection)
    for version in versions:
        if not connection.engine.has_table(version.version, schema=version.dataset):
            continue

        connection.execute(
            sa.text(
                f"""ALTER TABLE "{version.dataset}"."{version.version}"
               DROP COLUMN version
            """
            )
        )


@click.command()
@click.argument("operation", type=click.Choice(["upgrade", "downgrade"]))
def migrate(operation):
    db_user = os.environ["DB_USERNAME"]
    db_pass = os.environ["DB_PASSWORD"]
    db_host = os.environ["DB_HOST"]
    db_port = os.environ["DB_PORT"]
    db_name = os.environ["DB_NAME"]
    engine = sa.create_engine(
        f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"  # pragma: allowlist secret
    )

    with engine.connect() as connection:
        if operation == "upgrade":
            upgrade(connection)
        elif operation == "downgrade":
            downgrade(connection)
        else:
            raise ValueError("Operation not supported.")


if __name__ == "__main__":
    migrate()
