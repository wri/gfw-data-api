from .base import Base, db


class Asset(Base):
    __tablename__ = 'assets'
    dataset = db.Column(db.String, nullable=False)
    version = db.Column(db.String, nullable=False)
    asset_type = db.Column(db.String, nullable=False)
    asset_uri = db.Column(db.String, primary_key=True)
    # status = db.Column(db.String)
    metadata = db.Column(db.JSONB)

    fk = db.ForeignKeyConstraint(["dataset", "version"], ["versions.dataset", "versions.version"], name="fk")
