"""Re-create geostore table.

Revision ID: 12a4acb31f39
Revises: 194b576ecd96
Create Date: 2020-07-15 13:45:05.802123
"""
import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "12a4acb31f39"  # pragma: allowlist secret
down_revision = "194b576ecd96"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade():
    op.drop_index("geostore_gfw_geostore_id_idx", table_name="geostore")
    op.execute("DROP TABLE geostore CASCADE;")

    op.create_table(
        "geostore",
        sa.Column(
            "gfw_geostore_id", postgresql.UUID(), primary_key=True, nullable=False
        ),
        sa.Column("gfw_geojson", sa.TEXT(), nullable=True),
        sa.Column("gfw_area__ha", sa.Numeric(), nullable=True),
        sa.Column("gfw_bbox", postgresql.ARRAY(sa.Numeric()), nullable=True),
        sa.Column(
            "created_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.Column(
            "updated_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
    )
    op.create_index(
        "geostore_gfw_geostore_id_idx",
        "geostore",
        ["gfw_geostore_id"],
        unique=False,
        postgresql_using="hash",
    )


def downgrade():
    op.drop_index("geostore_gfw_geostore_id_idx", table_name="geostore")
    op.execute("DROP TABLE geostore CASCADE;")

    op.create_table(
        "geostore",
        sa.Column(
            "gfw_geostore_id", postgresql.UUID(), primary_key=True, nullable=False
        ),
        sa.Column("gfw_geojson", sa.TEXT(), nullable=True),
        sa.Column("gfw_area__ha", sa.Numeric(), nullable=True),
        sa.Column(
            "gfw_bbox",
            geoalchemy2.types.Geometry(geometry_type="POLYGON", srid=4326),
            nullable=True,
        ),
        sa.Column(
            "created_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.Column(
            "updated_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
    )
    op.create_index(
        "geostore_gfw_geostore_id_idx",
        "geostore",
        ["gfw_geostore_id"],
        unique=False,
        postgresql_using="hash",
    )
    op.drop_index("idx_geostore_gfw_bbox", table_name="geostore")
