"""Add version column to all data tables.

Revision ID: 034993cf423b
Revises: 4763f4b8141a
Create Date: 2021-09-21 14:20:18.461600
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "034993cf423b"
down_revision = "4763f4b8141a"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def get_datasets():
    connection = op.gets_bind()
    datasets = connection.execute(sa.text("select * from public.versions"))
    return datasets


def upgrade():
    versions = get_datasets()
    connection = op.gets_bind()
    for version in versions:
        if not connection.engine.has_table(version.version, schema=version.dataset):
            continue

        op.add_column(
            version.version,
            sa.Column("version", sa.String(), nullable=True),
            schema=version.dataset,
        )
        op.execute(
            f"""UPDATE {version.dataset}."{version.version}"
                    SET version = '{version.version}'
                """
        )
        op.alter_column(
            version.version, "version", nullable=False, schema=version.dataset
        )


def downgrade():
    versions = get_datasets()
    connection = op.gets_bind()
    for version in versions:
        if not connection.engine.has_table(version.version, schema=version.dataset):
            continue

        op.drop_column(version.version, "version", schema=version.dataset)
