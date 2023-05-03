from .base import Base, db


class Asset(Base):
    __tablename__ = "assets"
    asset_id = db.Column(db.UUID, primary_key=True)
    dataset = db.Column(db.String, nullable=False)
    version = db.Column(db.String, nullable=False)
    asset_type = db.Column(db.String, nullable=False)
    asset_uri = db.Column(db.String, nullable=False)
    status = db.Column(db.String, nullable=False, default="pending")
    is_managed = db.Column(db.Boolean, nullable=False, default=True)
    is_default = db.Column(db.Boolean, nullable=False, default=False)
    is_downloadable = db.Column(db.Boolean, nullable=False, default=True)
    creation_options = db.Column(db.JSONB, nullable=False, default=dict())
    # metadata = db.Column(db.JSONB, nullable=False, default=dict())
    fields = db.Column(db.JSONB, nullable=False, default=list())
    extent = db.Column(db.JSONB, nullable=True, default=None)
    stats = db.Column(db.JSONB, nullable=True, default=None)
    change_log = db.Column(db.ARRAY(db.JSONB), nullable=False, default=list())

    fk = db.ForeignKeyConstraint(
        ["dataset", "version"],
        ["versions.dataset", "versions.version"],
        name="fk",
        onupdate="CASCADE",
        ondelete="CASCADE",
    )

    uq_asset_uri = db.UniqueConstraint("asset_uri", name="uq_asset_uri")
