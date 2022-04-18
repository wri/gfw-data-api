from .base import Base, db


class Alias(Base):
    __tablename__ = "aliases"
    alias = db.Column(db.String, primary_key=True)
    dataset = db.Column(db.String, primary_key=True)
    version = db.Column(db.String, nullable=False)

    fk = db.ForeignKeyConstraint(
        ["dataset", "version"],
        ["versions.dataset", "versions.version"],
        name="fk",
        onupdate="CASCADE",
        ondelete="CASCADE",
    )
