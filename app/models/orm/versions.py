from .base import Base, db


class Version(Base):
    __tablename__ = "versions"
    dataset = db.Column(db.String, primary_key=True)
    version = db.Column(db.String, primary_key=True)
    is_latest = db.Column(db.Boolean, nullable=False, default=False)
    is_mutable = db.Column(db.Boolean, nullable=False, default=False)
    is_downloadable = db.Column(db.Boolean, nullable=False, default=True)
    # source_type = db.Column(db.String, nullable=False)
    # source_uri = db.Column(db.ARRAY(db.String), default=list())
    status = db.Column(db.String, nullable=False, default="pending")
    # has_geostore = db.Column(db.Boolean, nullable=False, default=False)
    # metadata = db.Column(db.JSONB, default=dict())
    change_log = db.Column(db.ARRAY(db.JSONB), default=list())
    # creation_options = db.Column(db.JSONB, default=dict())

    fk = db.ForeignKeyConstraint(
        ["dataset"],
        ["datasets.dataset"],
        name="fk",
        onupdate="CASCADE",
        ondelete="CASCADE",
    )
