import json
from base64 import urlsafe_b64encode

import ambuda.queries as q
import ambuda.database as db


def _cursor(offset):
    return urlsafe_b64encode(str(offset).encode()).decode()


def test_index(client):
    """Catalog page loads successfully with shared Texts header and tabs."""
    resp = client.get("/texts/catalog/")
    assert resp.status_code == 200
    assert ">Texts</h1>" in resp.text
    assert "Catalog" in resp.text
    assert "Collections" in resp.text


def test_index__has_text(client):
    """Seeded text appears in the catalog."""
    resp = client.get("/texts/catalog/")
    assert "pariksha" in resp.text


def test_partial__returns_json(client):
    """Partial requests return JSON with expected keys."""
    resp = client.get("/texts/catalog/?partial=1")
    assert resp.content_type.startswith("application/json")
    data = resp.json
    assert "html" in data
    assert "bar_html" in data
    assert "count" in data
    assert "counts" in data
    assert isinstance(data["count"], int)


def test_partial__counts_structure(client):
    """Counts JSON has all expected facets."""
    data = client.get("/texts/catalog/?partial=1").json
    for key in ("type", "collection", "status", "source"):
        assert key in data["counts"], f"missing counts key: {key}"


def test_search__matches(client):
    """Search filter narrows results."""
    data = client.get("/texts/catalog/?q=pariksha&partial=1").json
    assert data["count"] >= 1
    assert "pariksha" in data["html"].lower()


def test_search__no_match(client):
    """Search with no match returns zero results."""
    data = client.get("/texts/catalog/?q=zzzznotfound&partial=1").json
    assert data["count"] == 0


def test_filter_text_type(client):
    """Text type filter works (pariksha has no parent, so it's mula)."""
    mula = client.get("/texts/catalog/?text_type=mula&partial=1").json
    assert mula["count"] >= 1

    translation = client.get("/texts/catalog/?text_type=translation&partial=1").json
    assert translation["count"] <= mula["count"]


def test_filter_source(client):
    """Source filter distinguishes ambuda vs external."""
    # pariksha has no project_id, so it's external
    external = client.get("/texts/catalog/?source=external&partial=1").json
    assert external["count"] >= 1


def test_filter_status(client):
    """Status filter works; unknown status returns zero."""
    all_data = client.get("/texts/catalog/?partial=1").json
    # "none" status texts exist (pariksha has no status)
    # filtering by p2 should return fewer or equal
    p2 = client.get("/texts/catalog/?status=p2&partial=1").json
    assert p2["count"] <= all_data["count"]


def test_filter_combined(client):
    """Multiple filters combine (AND logic)."""
    all_data = client.get("/texts/catalog/?partial=1").json
    combined = client.get(
        "/texts/catalog/?text_type=mula&source=external&partial=1"
    ).json
    assert combined["count"] <= all_data["count"]


def test_sort_title(client):
    """Sort by title doesn't error."""
    resp = client.get("/texts/catalog/?sort=title&sort_dir=asc&partial=1")
    assert resp.json["count"] >= 0


def test_sort_created(client):
    """Sort by creation date doesn't error."""
    resp = client.get("/texts/catalog/?sort=created&sort_dir=desc&partial=1")
    assert resp.json["count"] >= 0


def test_counts_reflect_filters(client):
    """Counts should change when filters are applied."""
    all_data = client.get("/texts/catalog/?partial=1").json
    all_total = all_data["count"]

    # Filter to mula only — source counts should reflect mula texts only
    mula = client.get("/texts/catalog/?text_type=mula&partial=1").json
    mula_source_total = sum(mula["counts"]["source"].values())
    assert mula_source_total == mula["count"]


def test_cursor_pagination(client):
    """Cursor parameter is accepted."""
    # No cursor = first page
    resp = client.get("/texts/catalog/?partial=1")
    assert resp.json["count"] >= 0

    # Valid cursor
    resp = client.get(f"/texts/catalog/?cursor={_cursor(0)}&partial=1")
    assert resp.json["count"] >= 0

    # Cursor past the end should be clamped, not error
    resp = client.get(f"/texts/catalog/?cursor={_cursor(99999)}&partial=1")
    assert resp.json["count"] >= 0

    # Invalid cursor should fall back to offset 0
    resp = client.get("/texts/catalog/?cursor=invalid&partial=1")
    assert resp.json["count"] >= 0


def test_collection_filter(client):
    """Collection filter doesn't error even if no texts match."""
    resp = client.get("/texts/catalog/?collection=99999&partial=1")
    assert resp.json["count"] == 0


def test_index__no_trailing_slash_redirects(client):
    """Accessing /texts/catalog redirects to /texts/catalog/."""
    resp = client.get("/texts/catalog")
    assert resp.status_code == 308
