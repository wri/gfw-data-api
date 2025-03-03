from .base import Base, db


class AnalysisResult(Base):
    __tablename__ = "analysis_result"
    id = db.Column(db.UUID, primary_key=True)
    endpoint = db.Column(db.String)
    result = db.Column(db.JSONB)
    metadata = db.Column(db.JSONB)
    status = db.Column(db.String)
    requested_by = db.Column(
        db.UUID, db.ForeignKey("api_keys.api_key", name="api_key_fk")
    )
    error = db.Column(db.String)
