from asyncpg import UniqueViolationError

from app.errors import RecordAlreadyExistsError, RecordNotFoundError
from app.models.orm.aliases import Alias as ORMAlias


async def create_alias(alias: str, dataset: str, version: str) -> ORMAlias:
    try:
        new_alias: ORMAlias = await ORMAlias.create(
            alias=alias, dataset=dataset, version=version
        )
    except UniqueViolationError:
        raise RecordAlreadyExistsError(
            f"Alias {alias} already exists for dataset {dataset}"
        )

    return new_alias


async def get_alias(dataset: str, alias: str) -> ORMAlias:
    alias_record: ORMAlias = await ORMAlias.get([alias, dataset])
    if alias_record is None:
        raise RecordNotFoundError(f"Could not find requested alias {alias}.")

    return alias_record


async def delete_alias(dataset, alias: str) -> ORMAlias:
    alias_record: ORMAlias = await get_alias(dataset, alias)
    await ORMAlias.delete.where(ORMAlias.dataset == dataset).where(
        ORMAlias.alias == alias
    ).gino.status()

    return alias_record
