from .base import Base, db
from .mixins import MetadataMixin


class VersionMetadata(Base, MetadataMixin):
    __tablename__ = "version_metadata"

    id = db.Column(db.UUID, primary_key=True)
    dataset = db.Column(db.String, nullable=False)
    version = db.Column(db.String, nullable=False)
    content_date = db.Column(db.Date)
    content_start_date = db.Column(db.Date)
    content_end_date = db.Column(db.Date)
    last_update = db.Column(db.Date)
    description = db.Column(db.String)

    dataset_fk = db.ForeignKeyConstraint(
        ["dataset", "version"],
        ["versions.dataset", "versions.version"],
        name="dataset_fk",
        onupdate="CASCADE",
        ondelete="CASCADE",
    )
    _unique_dataset_version = db.UniqueConstraint(
        "dataset", "version", name="dataset_version_uq"
    )
