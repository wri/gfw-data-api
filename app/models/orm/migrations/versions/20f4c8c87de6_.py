"""empty message.

Revision ID: 20f4c8c87de6
Revises: 19a2413be4d8
Create Date: 2020-12-22 18:03:02.627614
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20f4c8c87de6"  # pragma: allowlist secret
down_revision = "19a2413be4d8"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "assets",
        sa.Column(
            "extent",
            postgresql.JSONB(astext_type=sa.Text()),
            default=sa.text("'{}'"),
            server_default=sa.text("'{}'"),
            nullable=True,
        ),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("assets", "extent")
    # ### end Alembic commands ###
