"""Add is_downloadable columns to datasets, versions and assets.

Revision ID: 4763f4b8141a
Revises: d62a9b15f844
Create Date: 2021-07-13 01:10:24.418512
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy import column, table

# revision identifiers, used by Alembic.
revision = "4763f4b8141a"  # pragma: allowlist secret
down_revision = "d62a9b15f844"  # pragma: allowlist secret
branch_labels = None
depends_on = None

tables = ["datasets", "versions", "assets"]
column_name = "is_downloadable"


def upgrade():
    for table_name in tables:

        op.add_column(table_name, sa.Column(column_name, sa.Boolean()))

        t = table(table_name, column(column_name))

        op.execute(
            t.update().where(t.c.is_downloadable is None).values(is_downloadable=True)
        )


def downgrade():
    for table_name in tables:
        op.drop_column(table_name, column_name)
