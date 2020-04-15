from .base import Base, db


class Version(Base):
    __tablename__ = 'versions'
    dataset = db.Column(db.String, primary_key=True)
    version = db.Column(db.String, primary_key=True)
    is_latest = db.Column(db.Boolean, default=False)
    source_type = db.Column(db.String, nullable=False)
    has_vector_tile_cache = db.Column(db.Boolean, default=False)
    has_raster_tile_cache = db.Column(db.Boolean, default=False)
    has_geostore = db.Column(db.Boolean, default=False)
    has_feature_info = db.Column(db.Boolean, default=False)
    has_10_40000_tiles = db.Column(db.Boolean, default=False)
    has_90_27008_tiles = db.Column(db.Boolean, default=False)
    has_90_9876_tiles = db.Column(db.Boolean, default=False)
    metadata = db.Column(db.JSONB)

    fk = db.ForeignKeyConstraint(["dataset"], ["datasets.dataset"], name="fk")
