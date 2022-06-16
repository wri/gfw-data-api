from typing import List

from app.crud.datasets import get_datasets
from app.models.orm.datasets import Dataset as ORMDataset


async def paginate_datasets(crud_impl=get_datasets,
                            size: int = None,
                            page: int = 0) -> List[ORMDataset]:
    return await crud_impl(size, page)
