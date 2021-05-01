from datetime import datetime, timedelta

import pytest

from app.authentication.api_keys import api_key_is_valid
from tests_v2.fixtures.authentication.api_keys import BAD_DOMAINS, GOOD_DOMAINS


@pytest.mark.parametrize(
    "origin",
    ["www.globalforestwatch.org", "pro.globalforestwatch.org", "globalforestwatch.org"],
)
def test_api_key_is_valid_good_domains(origin):
    domains = GOOD_DOMAINS
    expiration_date = datetime.now() + timedelta(1000)

    assert api_key_is_valid(domains, expiration_date, origin)


@pytest.mark.parametrize("origin", BAD_DOMAINS)
def test_api_key_is_valid_bad_origin(origin):
    domains = GOOD_DOMAINS
    expiration_date = datetime.now() + timedelta(1000)

    assert not api_key_is_valid(domains, expiration_date, origin)


def test_api_key_is_valid_bad_date():
    origin = "wwww.globalforestwatch.org"
    domains = GOOD_DOMAINS
    expiration_date = datetime.now() - timedelta(1000)

    assert not api_key_is_valid(domains, expiration_date, origin)
