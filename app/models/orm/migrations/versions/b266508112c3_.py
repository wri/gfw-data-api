"""empty message
Revision ID: b266508112c3
Revises: 6e56faf9e16f
Create Date: 2020-06-18 02:59:32.807850
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b266508112c3"  # pragma: allowlist secret
down_revision = "6e56faf9e16f"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("assets", sa.Column("is_default", sa.Boolean(), nullable=False))
    op.drop_constraint("uq_asset_type", "assets", type_="unique")
    op.drop_constraint("fk", "assets", type_="foreignkey")
    op.create_foreign_key(
        "fk",
        "assets",
        "versions",
        ["dataset", "version"],
        ["dataset", "version"],
        onupdate="CASCADE",
        ondelete="CASCADE",
    )
    op.drop_constraint("fk", "versions", type_="foreignkey")
    op.create_foreign_key(
        "fk",
        "versions",
        "datasets",
        ["dataset"],
        ["dataset"],
        onupdate="CASCADE",
        ondelete="CASCADE",
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint("fk", "versions", type_="foreignkey")
    op.create_foreign_key("fk", "versions", "datasets", ["dataset"], ["dataset"])
    op.drop_constraint("fk", "assets", type_="foreignkey")
    op.create_foreign_key(
        "fk", "assets", "versions", ["dataset", "version"], ["dataset", "version"]
    )
    op.create_unique_constraint(
        "uq_asset_type", "assets", ["dataset", "version", "asset_type"]
    )
    op.drop_column("assets", "is_default")
    # ### end Alembic commands ###
