"""Smoke test that rate limiting returns 429 when limits are exceeded."""

import pytest

from ambuda.rate_limit import limiter


@pytest.fixture(autouse=True)
def _enable_rate_limiting(flask_app):
    """Temporarily enable rate limiting for tests in this module."""
    limiter.enabled = True
    limiter._storage.reset()
    yield
    limiter.enabled = False


def test_sign_in_rate_limit(client):
    for _ in range(10):
        resp = client.post(
            "/sign-in",
            data={"username": "nobody", "password": "badpassword"},
        )
        assert resp.status_code != 429

    resp = client.post(
        "/sign-in",
        data={"username": "nobody", "password": "badpassword"},
    )
    assert resp.status_code == 429


def test_get_request_not_limited(client):
    for _ in range(15):
        resp = client.get("/sign-in")
        assert resp.status_code == 200
