from .base import Base, db


class Task(Base):
    __tablename__ = "tasks"
    task_id = db.Column(db.UUID, primary_key=True)
    asset_id = db.Column(db.UUID, nullable=False)
    status = db.Column(db.String, nullable=False, default="pending")

    change_log = db.Column(db.ARRAY(db.JSONB), default=list())

    fk = db.ForeignKeyConstraint(
        ["asset_id"],
        ["assets.asset_id"],
        name="fk",
        onupdate="CASCADE",
        ondelete="CASCADE",
    )
