"""Adding dataset version alias table.

Revision ID: a5787f2eefe5
Revises: 4763f4b8141a
Create Date: 2021-09-27 22:12:26.964711
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a5787f2eefe5"
down_revision = "4763f4b8141a"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "aliases",
        sa.Column("alias", sa.String(), nullable=False),
        sa.Column("dataset", sa.String(), nullable=False),
        sa.Column("version", sa.String(), nullable=False),
        sa.Column(
            "created_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.Column(
            "updated_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.ForeignKeyConstraint(
            ["dataset", "version"],
            ["versions.dataset", "versions.version"],
            name="fk",
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("dataset", "alias"),
    )


def downgrade():
    op.drop_table("aliases")
