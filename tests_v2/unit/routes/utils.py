from typing import Dict


def assert_jsend(resp_obj: Dict):
    assert resp_obj.get("status") in ("success", "error", "failed")
    if resp_obj.get("status") == "success":
        assert resp_obj.get("data") is not None
    else:
        assert resp_obj.get("message") is not None
