"""Set is_downloadable columns to not nullable.

Revision ID: 8f91123e430e
Revises: 4763f4b8141a
Create Date: 2021-07-15 21:17:01.375607
"""
from alembic import op
from sqlalchemy import column, table

# revision identifiers, used by Alembic.
revision = "8f91123e430e"  # pragma: allowlist secret
down_revision = "4763f4b8141a"  # pragma: allowlist secret
branch_labels = None
depends_on = None

tables = ["datasets", "versions", "assets"]
column_name = "is_downloadable"


def upgrade():
    for table_name in tables:
        t = table(table_name, column(column_name))
        op.execute(
            t.update().where(t.c.is_downloadable is None).values(is_downloadable=True)
        )
        op.alter_column(table_name, column_name, nullable=False)


def downgrade():
    for table_name in tables:
        op.alter_column(table_name, column_name, nullable=True)
