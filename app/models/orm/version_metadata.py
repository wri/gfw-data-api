from .base import Base, db
from .mixins import MetadataMixin


class VersionMetadata(Base, MetadataMixin):
    __tablename__ = "version_metadata"

    metadata_id = db.Column(db.String, primary_key=True)
    dataset = db.Column(db.String, nullable=False)
    version = db.Column(db.String, nullable=False)
    creation_date = db.Column(db.Date)
    content_start_date = db.Column(db.Date)
    content_end_date = db.Column(db.Date)
    last_update = db.Column(db.Date)
    description = db.Column(db.String)

    fk = db.ForeignKeyConstraint(
        ["dataset", "version"],
        ["versions.dataset", "versions.version"],
        name="fk",
        onupdate="CASCADE",
        ondelete="CASCADE",
    )
