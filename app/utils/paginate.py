from math import ceil
from typing import List, Optional, Tuple

from app.models.orm.base import Base as ORMBase
from app.models.pydantic.pagination import PaginationLinks, PaginationMeta


def _build_link(request_url, page, size) -> str:
    return f"{request_url}?page[number]={page}&page[size]={size}"


def _has_previous(page) -> bool:
    return page > 1


def _has_next(page, total_pages) -> bool:
    return total_pages > page


def _create_pagination_links(
    request_url: str, size: int, page: int, total_pages: int
) -> PaginationLinks:
    if _has_previous(page):
        prev_page = _build_link(request_url, page - 1, size)
    else:
        prev_page = ""

    if _has_next(page, total_pages):
        next_page = _build_link(request_url, page + 1, size)
    else:
        next_page = ""

    return PaginationLinks(
        self=_build_link(request_url, page, size),
        first=_build_link(request_url, 1, size),
        last=_build_link(request_url, total_pages, size),
        prev=prev_page,
        next=next_page,
    )


def _create_pagination_meta(size: int, total_items: int) -> PaginationMeta:
    assert size > 0
    total_pages: int = ceil(total_items / size) if total_items > 0 else 1
    return PaginationMeta(size=size, total_items=total_items, total_pages=total_pages)


def _calculate_offset(page: int, size: int) -> int:
    assert page > 0
    return size * (page - 1)


async def paginate_collection(
    paged_items_fn,
    item_count_fn,
    request_url="",
    size: Optional[int] = None,
    page: Optional[int] = None,
) -> Tuple[List[ORMBase], Optional[PaginationLinks], Optional[PaginationMeta]]:

    page_size: int = size if size is not None else 10
    page_number: int = page if page is not None else 1

    data = await paged_items_fn(size, _calculate_offset(page_number, page_size))

    if size is None and page is None:
        return data, None, None

    total_datasets = await item_count_fn()

    meta = _create_pagination_meta(size=page_size, total_items=total_datasets)

    if page_number > meta.total_pages:
        raise ValueError(
            f"Page number {page_number} is larger than the total page count: {meta.total_pages}"
        )

    links = _create_pagination_links(
        request_url=request_url,
        size=page_size,
        page=page_number,
        total_pages=meta.total_pages,
    )

    return data, links, meta
