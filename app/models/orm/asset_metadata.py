from .base import db


class AssetMetadata(db.Model):
    __tablename__ = "asset_metadata"

    id = db.Column(db.UUID, primary_key=True)
    asset_id = db.Column(
        db.UUID, db.ForeignKey("assets.asset_id", name="asset_id_fk"), unique=True
    )
    # dataset_metadata_id = db.Column(
    #     db.UUID, db.ForeignKey("dataset_metadata.id", name="dataset_metadata_id_fk")
    # )
    # version_metadata_id = db.Column(
    #     db.UUID, db.ForeignKey("version_metadata.id", name="version_metadata_id_fk")
    # )
    resolution = db.Column(db.Numeric)
    min_zoom = db.Column(db.Integer)
    max_zoom = db.Column(db.Integer)
    tags = db.Column(db.String)


class FieldMetadata(db.Model):
    __tablename__ = "field_metadata"

    asset_metadata_id = db.Column(
        db.UUID,
        db.ForeignKey(
            "asset_metadata.id",
            name="asset_metadata_id_fk",
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        primary_key=True,
    )
    name = db.Column(db.String, primary_key=True)
    description = db.Column(db.String)
    alias = db.Column(db.String)
    unit = db.Column(db.String)
    data_type = db.Column(db.String)
    is_feature_info = db.Column(db.Boolean, default=True)
    is_filter = db.Column(db.Boolean, default=True)


class RasterBandMetadata(db.Model):
    __tablename__ = "raster_band_metadata"

    asset_metadata_id = db.Column(
        db.UUID,
        db.ForeignKey(
            "asset_metadata.id",
            name="asset_metadata_id_fk",
            onupdate="CASCADE",
            ondelete="CASCADE",
            primary_key=True,
        ),
    )
    pixel_meaning = db.Column(db.String, primary_key=True)
    description = db.Column(db.String)
    alias = db.Column(db.String)
    data_type = db.Column(db.String)
    unit = db.Column(db.String)
    compression = db.Column(db.String)
    no_data_value = db.Column(db.String)
    statistics = db.Column(db.JSONB)
    values_table = db.Column(db.JSONB)
