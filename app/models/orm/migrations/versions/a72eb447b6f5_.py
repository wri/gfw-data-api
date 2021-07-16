"""Set is downloadable to not null.

Revision ID: a72eb447b6f5
Revises: 8f91123e430e
Create Date: 2021-07-16 03:20:31.158342
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "a72eb447b6f5"  # pragma: allowlist secret
down_revision = "8f91123e430e"  # pragma: allowlist secret
branch_labels = None
depends_on = None


tables = ["datasets", "versions", "assets"]
column_name = "is_downloadable"


def upgrade():
    for table_name in tables:
        op.alter_column(table_name, column_name, nullable=False)


def downgrade():
    for table_name in tables:
        op.alter_column(table_name, column_name, nullable=True)
