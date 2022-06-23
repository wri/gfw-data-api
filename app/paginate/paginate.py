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
    if (page < 1) or (page > total_pages and page > 1):
        raise ValueError  # TODO add error message

    return PaginationLinks(
        f"{request_url}?page[number]={page}&page[size]={size}",
        f"{request_url}?page[number]=1&page[size]={size}",
        f"{request_url}?page[number]={total_pages or 1}&page[size]={size}",
        f"{request_url}?page[number]={(page - 1)}&page[size]={size}"
        if (page > 1)
        else "",
        f"{request_url}?page[number]={(page + 1)}&page[size]={size}"
        if (total_pages > page)
        else "",
    )


def _create_pagination_meta(size: int, total_items: int):
    assert size > 0
    return PaginationMeta(size, total_items, ceil(total_items / size))


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

    links = _create_pagination_links(
        request_url=request_url,
        size=page_size,
        page=page_number,
        total_pages=meta.total_pages,
    )

    return data, links, meta
