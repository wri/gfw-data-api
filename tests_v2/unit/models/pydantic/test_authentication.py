from itertools import cycle

import pytest
from pydantic import ValidationError

from app.models.pydantic.authentication import APIKeyRequestIn
from tests_v2.fixtures.authentication.api_keys import (
    BAD_DOMAINS,
    BAD_EMAILS,
    GOOD_DOMAINS,
    GOOD_EMAILS,
    GOOD_ORGANIZATIONS,
)


@pytest.mark.parametrize(
    "org,email,domain",
    list(zip(cycle(GOOD_ORGANIZATIONS), cycle(GOOD_EMAILS), GOOD_DOMAINS)),
)
def test_APIKeyRequestIn(org, email, domain):

    request = APIKeyRequestIn(
        alias="my alias", organization=org, email=email, domains=[domain]
    )
    assert request.organization == org
    assert request.email == email
    assert request.domains == [domain]


@pytest.mark.parametrize(
    "org,email,domain",
    list(zip(cycle([GOOD_ORGANIZATIONS[0]]), BAD_EMAILS, cycle([GOOD_DOMAINS[0]]))),
)
def test_APIKeyRequestIn_bad_email(org, email, domain):

    with pytest.raises(ValidationError):
        APIKeyRequestIn(
            alias="my alias", organization=org, email=email, domains=[domain]
        )


@pytest.mark.parametrize(
    "org,email,domain",
    list(zip(cycle([GOOD_ORGANIZATIONS[0]]), cycle([GOOD_EMAILS[0]]), BAD_DOMAINS)),
)
def test_APIKeyRequestIn_bad_domain(org, email, domain):

    with pytest.raises(ValidationError):
        APIKeyRequestIn(
            alias="my alias", organization=org, email=email, domains=[domain]
        )


@pytest.mark.parametrize(
    "org,email,domain",
    list(zip(cycle(GOOD_ORGANIZATIONS), cycle(GOOD_EMAILS), GOOD_DOMAINS)),
)
def test_APIKeyRequestIn_no_alias(org, email, domain):
    with pytest.raises(ValidationError):
        APIKeyRequestIn(alias=None, organization=org, email=email, domains=[domain])
