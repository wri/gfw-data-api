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
    if page > total_pages and page > 1:
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
    return PaginationMeta(size, total_items, ceil(total_items / size))


async def paginate_datasets(
    crud_impl=get_datasets,
    datasets_count_impl=count_datasets,
    request_url="",
    size: Optional[int] = None,
    page: Optional[int] = 0,
) -> Tuple[List[ORMDataset], Optional[PaginationLinks], Optional[PaginationMeta]]:
    data = await crud_impl(size, _calculate_offset(page, size))

    if size is None and page == 0:
        return data, None, None

    total_datasets = await datasets_count_impl()

    meta = _create_pagination_meta(size=size or 1, total_items=total_datasets)

    links = _create_pagination_links(
        request_url=request_url,
        size=size or 1,
        page=page or 1,
        total_pages=meta.total_pages,
    )

    return data, links, meta


def _calculate_offset(page, size):
    return (size or 1) * max(0, page - 1)
