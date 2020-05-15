import requests
from fastapi import Path, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer


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
