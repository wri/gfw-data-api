from itertools import cycle

import pytest
from pydantic import ValidationError

from app.models.pydantic.authentication import APIKeyRequestIn

GOOD_ORGANIZATIONS = ["WRI", "Global Forest Watch"]
GOOD_EMAILS = [
    "info@wri.org",
    "admin@globalforestwatch.org",
    "firstname.lastname@test.com",
]
GOOD_DOMAINS = [
    "www.globalforestwatch.org",
    "*.globalforestwatch.org",
    "globalforestwatch.org",
    "localhost",
]

BAD_EMAILS = ["not an email", "also_not@n-email", "nope", None]
BAD_DOMAINS = ["www.*.com", "*", "www.test*.org", "www.test.*", "*.com"]


@pytest.mark.parametrize(
    "org,email,domain",
    list(zip(cycle(GOOD_ORGANIZATIONS), cycle(GOOD_EMAILS), GOOD_DOMAINS)),
)
def test_APIKeyRequestIn(org, email, domain):

    request = APIKeyRequestIn(organization=org, email=email, domains=[domain])
    assert request.organization == org
    assert request.email == email
    assert request.domains == [domain]


@pytest.mark.parametrize(
    "org,email,domain",
    list(zip(cycle([GOOD_ORGANIZATIONS[0]]), BAD_EMAILS, cycle([GOOD_DOMAINS[0]]))),
)
def test_APIKeyRequestIn_bad_email(org, email, domain):

    with pytest.raises(ValidationError):
        APIKeyRequestIn(organization=org, email=email, domains=[domain])


@pytest.mark.parametrize(
    "org,email,domain",
    list(zip(cycle([GOOD_ORGANIZATIONS[0]]), cycle([GOOD_EMAILS[0]]), BAD_DOMAINS)),
)
def test_APIKeyRequestIn_bad_domain(org, email, domain):

    with pytest.raises(ValidationError):
        APIKeyRequestIn(organization=org, email=email, domains=[domain])
