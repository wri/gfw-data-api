import pytest
from fastapi import HTTPException

from app.routes import is_admin, is_service_account, who_am_i


@pytest.mark.asyncio
async def test_is_admin():

    message = ""
    try:
        await is_admin("my_fake_token")
    except HTTPException as e:
        message = e.detail

    assert message == "Unauthorized"


@pytest.mark.asyncio
async def test_is_service_account():

    message = ""
    try:
        await is_service_account("my_fake_token")
    except HTTPException as e:
        message = e.detail

    assert message == "Unauthorized"


def test_who_am_i():
    response = who_am_i("my_fake_token")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_login(async_client):
    response = await async_client.post(
        "/token", data={"username": "name", "password": "secret"}
    )
    assert response.status_code == 401
