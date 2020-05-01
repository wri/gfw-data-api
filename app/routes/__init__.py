from typing import Union, Dict, Any

import requests
from fastapi import Path, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

from ..application import db


VERSION_REGEX = r"^v\d{1,8}\.?\d{1,3}\.?\d{1,3}$|^latest$"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")


async def dataset_dependency(dataset: str = Path(..., title="Dataset")):
    return dataset


async def version_dependency(
    version: str = Path(..., title="Dataset version", regex=VERSION_REGEX)
):

    # if version == "latest":
    #     version = ...

    return version


async def update_data(
    row: db.Model, input_data: Union[BaseModel, Dict[str, Any]]  # type: ignore
) -> db.Model:  # type: ignore
    """
    Merge updated metadata filed with existing fields
    """
    if isinstance(input_data, BaseModel):
        input_data = input_data.dict(skip_defaults=True)

    # Make sure, existing metadata not mentioned in request remain untouched
    if "metadata" in input_data.keys():
        metadata = row.metadata
        metadata.update(input_data["metadata"])
        input_data["metadata"] = metadata

    # new_row = model.from_orm(row)
    # new_row.metadata = metadata

    await row.update(**input_data).apply()

    return row


async def is_admin(token: str = Depends(oauth2_scheme)) -> bool:
    """
    Calls GFW API to authorize user
    """

    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://production-api.globalforestwatch.org/auth/check-logged"
    response = requests.get(url, headers=headers)

    if response.status_code != 200 and response.status_code != 401:
        raise HTTPException(
            status_code=500, detail="Call to authorization server failed"
        )

    if response.status_code == 401 or not (
        response.json()["role"] == "ADMIN"
        and "gfw" in response.json()["extraUserData"]["apps"]
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")
    else:
        return True
