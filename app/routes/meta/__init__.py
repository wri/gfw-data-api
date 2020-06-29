from app.crud.versions import get_version as _get_version
from app.errors import ClientError

from ...models.orm.versions import Version as ORMVersion


async def verify_version_status(dataset, version):
    orm_version: ORMVersion = await _get_version(dataset, version)

    if orm_version.status == "pending":
        raise ClientError(
            status_code=409,
            detail="Version status is currently `pending`. "
            "Please retry once version is in status `saved`",
        )
    elif orm_version.status == "failed":
        raise ClientError(
            status_code=400, detail="Version status is `failed`. Cannot add any assets."
        )
