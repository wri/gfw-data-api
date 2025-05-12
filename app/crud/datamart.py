"""CRUD functions for data mart analysis results."""

import json
import uuid

from app.errors import RecordNotFoundError
from app.models.orm.datamart import AnalysisResult
from app.models.pydantic.datamart import DataMartResource


async def save_result(result_data: DataMartResource) -> AnalysisResult:

    analysis_result: AnalysisResult = await AnalysisResult.create(
        **json.loads(result_data.json(by_alias=False))
    )

    return analysis_result


async def get_result(result_id: uuid.UUID) -> AnalysisResult:
    analysis_result: AnalysisResult = await AnalysisResult.get([result_id])
    if analysis_result is None:
        raise RecordNotFoundError(f"Could not find requested result {result_id}")

    return analysis_result


async def update_result(result_id: uuid.UUID, result_data) -> AnalysisResult:
    analysis_result: AnalysisResult = await get_result(result_id)
    await analysis_result.update(**json.loads(result_data.json(by_alias=False))).apply()

    return analysis_result


async def delete_result(result_id: uuid.UUID) -> AnalysisResult:
    analysis_result: AnalysisResult = await get_result(result_id)
    await AnalysisResult.delete.where(AnalysisResult.id == result_id).gino.status()

    return analysis_result
