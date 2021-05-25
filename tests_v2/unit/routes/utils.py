from datetime import datetime
from typing import Dict


def assert_jsend(resp_obj: Dict):
    assert resp_obj.get("status") in ("success", "error", "failed")
    if resp_obj.get("status") == "success":
        assert resp_obj.get("data") is not None
    else:
        assert resp_obj.get("message") is not None


def assert_is_datetime(value: str):
    datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")
