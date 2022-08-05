"""add metadata table.

Revision ID: 04fcb4f2408a
Revises: 4763f4b8141a
Create Date: 2022-01-20 20:25:58.995306
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "04fcb4f2408a"  # pragma: allowlist secret
down_revision = "4763f4b8141a"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "dataset_metadata",
        sa.Column(
            "id",
            postgresql.UUID(),
            nullable=False,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column("title", sa.String()),
        sa.Column("dataset", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("license", sa.String(), nullable=False),
        sa.Column("data_language", sa.String(), nullable=False),
        sa.Column("overview", sa.String(), nullable=False),
        sa.Column("function", sa.String()),
        sa.Column("cautions", sa.String()),
        sa.Column("key_restrictions", sa.String()),
        sa.Column("keywords", sa.ARRAY(sa.String)),
        sa.Column("why_added", sa.String()),
        sa.Column("learn_more", sa.String()),
        sa.Column("resolution", sa.Numeric()),
        sa.Column("geographic_coverage", sa.String()),
        sa.Column("update_frequency", sa.String()),
        sa.Column("citation", sa.String()),
        sa.Column("scale", sa.String()),
        sa.Column(
            "created_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.Column(
            "updated_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.ForeignKeyConstraint(
            ["dataset"],
            ["datasets.dataset"],
            name="fk",
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dataset", name="dataset_uq"),
    )

    op.create_table(
        "version_metadata",
        sa.Column(
            "id",
            postgresql.UUID(),
            nullable=False,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column("title", sa.String()),
        sa.Column("dataset", sa.String(), nullable=False),
        sa.Column("version", sa.String(), nullable=False),
        sa.Column("dataset_metadata_id", postgresql.UUID(), nullable=True),
        sa.Column("creation_date", sa.Date(), nullable=False),
        sa.Column("content_start_date", sa.Date(), nullable=False),
        sa.Column("content_end_date", sa.Date(), nullable=False),
        sa.Column("last_update", sa.Date()),
        sa.Column("description", sa.String()),
        sa.Column("resolution", sa.Numeric()),
        sa.Column("geographic_coverage", sa.String()),
        sa.Column("update_frequency", sa.String()),
        sa.Column("citation", sa.String()),
        sa.Column("scale", sa.String()),
        sa.Column(
            "created_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.Column(
            "updated_on", sa.DateTime(), server_default=sa.text("now()"), nullable=True
        ),
        sa.ForeignKeyConstraint(
            ["dataset", "version"],
            ["versions.dataset", "versions.version"],
            name="dataset_fk",
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["dataset_metadata_id"],
            ["dataset_metadata.id"],
            name="dataset_metadata_id_fk",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dataset", "version", name="dataset_version_uq"),
    )

    op.create_table(
        "asset_metadata",
        sa.Column(
            "id",
            postgresql.UUID(),
            nullable=False,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column("asset_id", postgresql.UUID(), nullable=False),
        sa.Column("resolution", sa.Numeric()),
        sa.Column("min_zoom", sa.Integer()),
        sa.Column("max_zoom", sa.Integer()),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["asset_id"],
            ["assets.asset_id"],
            name="asset_id_fk",
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("asset_id", name="asset_id_uq"),
    )

    op.create_table(
        "field_metadata",
        sa.Column("asset_metadata_id", postgresql.UUID(), nullable=True),
        sa.Column("name", sa.String()),
        sa.Column("description", sa.String()),
        sa.Column("alias", sa.String()),
        sa.Column("unit", sa.String()),
        sa.Column("is_feature_info", sa.Boolean(), default=True),
        sa.Column("is_filter", sa.Boolean(), default=True),
        sa.Column("data_type", sa.String()),
        sa.PrimaryKeyConstraint("asset_metadata_id", "name"),
        sa.ForeignKeyConstraint(
            ["asset_metadata_id"],
            ["asset_metadata.id"],
            name="asset_metadata_id_fk",
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )

    op.create_table(
        "raster_band_metadata",
        sa.Column("asset_metadata_id", postgresql.UUID(), nullable=True),
        sa.Column("pixel_meaning", sa.String()),
        sa.Column("description", sa.String()),
        sa.Column("alias", sa.String()),
        sa.Column("unit", sa.String()),
        sa.Column("data_type", sa.String()),
        sa.Column("compression", sa.String()),
        sa.Column("no_data_value", sa.String()),
        sa.Column(
            "statistics",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "values_table",
            postgresql.ARRAY(postgresql.JSONB(astext_type=sa.Text())),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("asset_metadata_id", "pixel_meaning"),
        sa.ForeignKeyConstraint(
            ["asset_metadata_id"],
            ["asset_metadata.id"],
            name="asset_metadata_id_fk",
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )
    op.drop_column("datasets", "metadata")
    op.drop_column("versions", "metadata")
    op.drop_column("assets", "metadata")


def downgrade():
    op.drop_table("raster_band_metadata")
    op.drop_table("field_metadata")
    op.drop_table("asset_metadata")
    op.drop_table("version_metadata")
    op.drop_table("dataset_metadata")
    op.add_column(
        "datasets",
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()),
        ),
    )
    op.add_column(
        "versions",
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()),
        ),
    )
    op.add_column(
        "assets",
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()),
        ),
    )