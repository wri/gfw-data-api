from .base import Base, db


class Asset(Base):
    __tablename__ = 'assets'
    asset_id = db.Column(db.UUID, primary_key=True)
    dataset = db.Column(db.String, nullable=False)
    version = db.Column(db.String, nullable=False)
    asset_type = db.Column(db.String, nullable=False)
    asset_uri = db.Column(db.String, nullable=False)
    status = db.Column(db.String, nullable=False)
    is_managed = db.Column(db.Boolean, default=True)
    creation_options = db.Column(db.JSONB)
    history = db.Column(db.ARRAY(db.JSONB))
    metadata = db.Column(db.JSONB)
    change_log = db.Column(db.ARRAY(db.JSONB), default=list())

    fk = db.ForeignKeyConstraint(["dataset", "version"], ["versions.dataset", "versions.version"], name="fk")
