import pytest


@pytest.fixture(scope="module", autouse=True)
def crud_module_db(module_db):
    """auto use module db.

    Module level fixtures have cannot be async, because the event loop
    is closed between tests.
    """
    yield


@pytest.fixture(autouse=True)
def crud_init_db(init_db):
    """auto use init db."""
    yield
