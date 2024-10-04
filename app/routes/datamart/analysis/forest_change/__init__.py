"""
Forest Change analysis tools!

**Legend:**

⚠️ = _Alerts_

🔥 = _Fires_

🌳 = _Tree Cover Change_

----
"""
from fastapi import APIRouter

from app.routes.datamart.analysis.forest_change.tree_cover_change import tree_cover_change_router

datamart_analysis_forest_change_metadata_tags = [
    {"name": "Forest Change Analysis 📊", "description": __doc__},
]

forest_change_router = APIRouter(
    prefix="/forest_change",
    tags=["Forest Change Analysis 📊"]
)

forest_change_router.include_router(tree_cover_change_router)