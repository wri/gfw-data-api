import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
import requests


@pytest.mark.asyncio
async def test_curl(httpd):
    httpd_port = httpd.server_port
    requests.put(f"http://localhost:{httpd_port}", data=json.dumps({"hello": "world"}))
    # put_resp = requests.put(
    #     f"http://localhost:{httpd_port}", data=json.dumps({"hello": "world"})
    # )
    # put_resp_json = json.loads(put_resp.text)
    # logging.error(f"Put response body: {put_resp_json}")

    get_resp = requests.get(f"http://localhost:{httpd_port}")
    get_resp_list = get_resp.json()["requests"]
    # logging.error(f"Get response body: {get_resp.text}")

    assert len(get_resp_list) == 1
    assert get_resp_list[0]["body"] == {"hello": "world"}
