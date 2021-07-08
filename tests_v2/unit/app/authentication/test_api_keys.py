from datetime import datetime, timedelta

import pytest

from app.authentication.api_keys import api_key_is_valid
from tests_v2.fixtures.authentication.api_keys import BAD_DOMAINS, GOOD_DOMAINS


@pytest.mark.parametrize(
    "origin",
    [
        "www.globalforestwatch.org",
        "pro.globalforestwatch.org",
        "globalforestwatch.org",
        "https://www.globalforestwatch.org",
        "https://www.globalforestwatch.org:9000",
    ],
)
def test_api_key_is_valid_good_origins(origin):
    domains = GOOD_DOMAINS
    expiration_date = datetime.now() + timedelta(1000)

    assert api_key_is_valid(domains, expiration_date, origin)


@pytest.mark.parametrize("origin", BAD_DOMAINS)
def test_api_key_is_valid_bad_origin(origin):
    domains = GOOD_DOMAINS
    expiration_date = datetime.now() + timedelta(1000)

    assert not api_key_is_valid(domains, expiration_date, origin)


@pytest.mark.parametrize("origin", BAD_DOMAINS)
def test_api_key_is_valid_ignore_origin(origin):
    domains = list()
    expiration_date = datetime.now() + timedelta(1000)

    assert api_key_is_valid(domains, expiration_date, origin)


@pytest.mark.parametrize(
    "referrer",
    [
        "www.globalforestwatch.org",
        "http://pro.globalforestwatch.org",
        "https://globalforestwatch.org",
        "https://globalforestwatch.org/abc",
        "http://www.globalforestwatch.org/abc?a=b",
    ],
)
def test_api_key_is_valid_good_referrers(referrer):
    domains = GOOD_DOMAINS
    expiration_date = datetime.now() + timedelta(1000)

    assert api_key_is_valid(domains, expiration_date, referrer=referrer)


@pytest.mark.parametrize(
    "referrer",
    [
        "www.*.org",
        "http://www.globalforestwatch.test.org",
        "www.globalforestwatch.org/test",
    ],
)
def test_api_key_is_valid_bad_referrers(referrer):
    domains = GOOD_DOMAINS
    expiration_date = datetime.now() + timedelta(1000)

    assert not api_key_is_valid(domains, expiration_date, referrer=referrer)


@pytest.mark.parametrize(
    "referrer",
    ["www.globalforestwatch.org", "http://www.globalforestwatch.test.org"],
)
def test_api_key_is_valid_ignore_referrers(referrer):
    domains = list()
    expiration_date = datetime.now() + timedelta(1000)

    assert api_key_is_valid(domains, expiration_date, referrer=referrer)


def test_api_key_is_valid_bad_date():
    origin = "wwww.globalforestwatch.org"
    domains = GOOD_DOMAINS
    expiration_date = datetime.now() - timedelta(1000)

    assert not api_key_is_valid(domains, expiration_date, origin)


def test_api_key_is_valid_ignore_date():
    origin = "wwww.globalforestwatch.org"
    domains = GOOD_DOMAINS
    expiration_date = None

    assert api_key_is_valid(domains, expiration_date, origin)


def test_api_key_is_valid_ignore_all():
    origin = None
    referrer = None
    domains = list()
    expiration_date = None

    assert api_key_is_valid(domains, expiration_date, origin, referrer)
