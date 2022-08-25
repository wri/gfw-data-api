from math import ceil
from typing import List, NamedTuple, Optional, Tuple

from app.crud.datasets import count_datasets, get_datasets
from app.models.orm.datasets import Dataset as ORMDataset


class PaginationLinks(NamedTuple):
    self: str
    first: str
    last: str
    prev: str
    next: str


class PaginationMeta(NamedTuple):
    size: int
    total_items: int
    total_pages: int


def _create_pagination_links(
    request_url: str, size: int, page: int, total_pages: int
) -> PaginationLinks:
    size_param = f"page[size]={size}"
    return PaginationLinks(
        f"{request_url}?page[number]={page}&{size_param}",
        f"{request_url}?page[number]=1&{size_param}",
        f"{request_url}?page[number]={total_pages}&{size_param}",
        f"{request_url}?page[number]={(page - 1)}&{size_param}" if (page > 1) else "",
        f"{request_url}?page[number]={(page + 1)}&{size_param}"
        if (total_pages > page)
        else "",
    )


def _create_pagination_meta(size: int, total_items: int):
    assert size > 0
    total_pages: int = ceil(total_items / size) if total_items > 0 else 1
    return PaginationMeta(size, total_items, total_pages)


def _calculate_offset(page: int, size: int):
    assert page > 0
    return size * (page - 1)


async def paginate_datasets(
    paged_items_fn=get_datasets,
    item_count_fn=count_datasets,
    request_url="",
    size: Optional[int] = None,
    page: Optional[int] = None,
) -> Tuple[List[ORMDataset], Optional[PaginationLinks], Optional[PaginationMeta]]:

    page_size: int = size if size is not None else 1
    page_number: int = page if page is not None else 1

    data = await paged_items_fn(size, _calculate_offset(page_number, page_size))

    if size is None and page is None:
        return data, None, None

    total_datasets = await item_count_fn()

    meta = _create_pagination_meta(size=page_size, total_items=total_datasets)

    if page_number > meta.total_pages:
        raise ValueError(
            f"Given the page size of {page_size}, page number {page_number} is larger than the total page count: {meta.total_pages}"
        )

    links = _create_pagination_links(
        request_url=request_url,
        size=page_size,
        page=page_number,
        total_pages=meta.total_pages,
    )

    return data, links, meta
