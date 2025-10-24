from .base import Base, db


class AnalysisResult(Base):
    __tablename__ = "analysis_results"
    id = db.Column(db.UUID, primary_key=True)
    endpoint = db.Column(db.String)
    result = db.Column(db.JSONB)
    metadata = db.Column(db.JSONB)
    status = db.Column(db.String)
    requested_by = db.Column(
        db.UUID, db.ForeignKey("api_keys.api_key", name="api_key_fk")
    )
    message = db.Column(db.String)

    _api_keys_api_key_idx = db.Index(
        "analysis_results_id_idx", "id", postgresql_using="hash"
    )
