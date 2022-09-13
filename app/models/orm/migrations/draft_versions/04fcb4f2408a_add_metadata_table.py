"""add metadata table.

Revision ID: 04fcb4f2408a
Revises: 4763f4b8141a
Create Date: 2022-01-20 20:25:58.995306
"""

import re
from itertools import chain
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from app.models.pydantic.metadata import DatasetMetadata, VersionMetadata
from app.models.orm.dataset_metadata import DatasetMetadata as ORMDatasetMetadata
from app.models.orm.version_metadata import VersionMetadata as ORMVersionMetadata

# revision identifiers, used by Alembic.
revision = "04fcb4f2408a"  # pragma: allowlist secret
down_revision = "4763f4b8141a"  # pragma: allowlist secret
branch_labels = None
depends_on = None


def parse_resolution(resolution_str):
    if resolution_str is None:
        return None
    resolution_str = re.sub("\s+", "", resolution_str)
    units = ["degrees", "km", "meter", "hectare", "m"]
    if all(unit not in resolution_str for unit in units):
        return None

    parsed_res = resolution_str.lower().replace("\u00d7", "x").split("x")[0]
    try:
        numeric_res = float(re.sub(r"[^0-9.]", "", parsed_res))
    except ValueError:
        return None
    if "km" in resolution_str:
        return numeric_res * 1000

    if "degree" in resolution_str:
        return numeric_res * 111000

    if "hectare" in resolution_str:
        return (numeric_res * 1e4) ** 0.5

    return numeric_res


def get_metadata():
    connection = op.get_bind()
    datasets = connection.execute(
        sa.text("select dataset, metadata from public.datasets")
    ).fetchall()
    versions = connection.execute(
        sa.text("select dataset, version, metadata from public.versions")
    ).fetchall()
    dataset_metadata = [
        dict(
            dataset=dataset[0],
            **DatasetMetadata(
                **dict(
                    chain(
                        dataset[1].items(),
                        {
                            "resolution": parse_resolution(dataset[1].get("resolution"))
                        }.items(),
                    )
                )
            ).dict()
        )
        for dataset in datasets
        if dataset[1] is not None
        and not all(value is None for value in dataset[1].values())
    ]
    version_metadata = [
        dict(
            dataset=version[0],
            version=version[1],
            **VersionMetadata(
                **dict(
                    chain(
                        version[2].items(),
                        {
                            "resolution": parse_resolution(version[2].get("resolution"))
                        }.items(),
                    )
                )
            ).dict()
        )
        for version in versions
        if version[2] is not None
        and not all(value is None for value in version[2].values())
    ]

    return dataset_metadata, version_metadata


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
        sa.Column("source", sa.String(), nullable=True),
        sa.Column("license", sa.String(), nullable=True),
        sa.Column("data_language", sa.String(), nullable=True),
        sa.Column("overview", sa.String(), nullable=True),
        sa.Column("function", sa.String()),
        sa.Column("cautions", sa.ARRAY(sa.String())),
        sa.Column("key_restrictions", sa.String()),
        sa.Column("tags", sa.ARRAY(sa.String)),
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
        sa.Column("creation_date", sa.Date(), nullable=True),
        sa.Column("content_start_date", sa.Date(), nullable=True),
        sa.Column("content_end_date", sa.Date(), nullable=True),
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
        sa.Column("tags", sa.ARRAY(sa.String)),
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
            postgresql.JSONB(astext_type=sa.Text()),
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

    dataset_metadata, version_metadata = get_metadata()
    connection = op.get_bind()
    if len(dataset_metadata):
        connection.execute(ORMDatasetMetadata.insert(), dataset_metadata)
    if len(version_metadata):
        connection.execute(ORMVersionMetadata.insert(), version_metadata)
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
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
        ),
    )
    op.add_column(
        "versions",
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
        ),
    )
    op.add_column(
        "assets",
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
        ),
    )
