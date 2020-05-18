import uuid

from .base import Base, db


class Asset(Base):
    __tablename__ = "assets"
    asset_id = db.Column(db.UUID, primary_key=True, default=uuid.uuid4())
    dataset = db.Column(db.String, nullable=False)
    version = db.Column(db.String, nullable=False)
    asset_type = db.Column(db.String, nullable=False)
    asset_uri = db.Column(db.String, nullable=False)
    status = db.Column(db.String, nullable=False, default="pending")
    is_managed = db.Column(db.Boolean, nullable=False, default=True)
    creation_options = db.Column(db.JSONB, default=dict())
    history = db.Column(db.ARRAY(db.JSONB), default=list())
    metadata = db.Column(db.JSONB, default=dict())
    change_log = db.Column(db.ARRAY(db.JSONB), default=list())

    fk = db.ForeignKeyConstraint(
        ["dataset", "version"], ["versions.dataset", "versions.version"], name="fk"
    )

    uq_asset_uri = db.UniqueConstraint("asset_uri", name="uq_asset_uri")
    uq_asset_type = db.UniqueConstraint(
        "dataset", "version", "asset_type", name="uq_asset_type"
    )
