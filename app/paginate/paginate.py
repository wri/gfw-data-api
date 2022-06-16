from typing import Dict, List, Optional, Tuple

from app.crud.datasets import get_datasets
from app.models.orm.datasets import Dataset as ORMDataset


async def paginate_datasets(
    crud_impl=get_datasets, size: int = None, page: int = 0
) -> Tuple[List[ORMDataset], Optional[Dict]]:
    data = await crud_impl(size, page)

    meta: Optional[Dict[str, int]] = None
    if size or page:
        meta = {}

    return data, meta
