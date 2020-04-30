from .base import Base, db


class Version(Base):
    __tablename__ = 'versions'
    dataset = db.Column(db.String, primary_key=True)
    version = db.Column(db.String, primary_key=True)
    is_latest = db.Column(db.Boolean, default=False)
    is_mutable = db.Column(db.Boolean, default=False)
    source_type = db.Column(db.String, nullable=False)
    source_uri = db.Column(db.ARRAY(db.String))
    has_vector_tile_cache = db.Column(db.Boolean, default=False)
    has_raster_tile_cache = db.Column(db.Boolean, default=False)
    has_geostore = db.Column(db.Boolean, default=False)
    has_feature_info = db.Column(db.Boolean, default=False)
    metadata = db.Column(db.JSONB)
    change_log = db.Column(db.ARRAY(db.JSONB), default=list())

    fk = db.ForeignKeyConstraint(["dataset"], ["datasets.dataset"], name="fk")
