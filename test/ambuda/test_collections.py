"""Tests for text collection query helpers and admin management routes."""

import json

import pytest

import ambuda.database as db
from ambuda.queries import all_descendant_ids, get_session, group_collections_by_parent


@pytest.fixture()
def collections(flask_app):
    """Create a small collection tree for testing:
    root
      ├── child_a
      │     └── grandchild
      └── child_b
    """
    with flask_app.app_context():
        session = get_session()
        root = db.TextCollection(slug="root", title="Root", order=1)
        session.add(root)
        session.flush()

        child_a = db.TextCollection(
            slug="child-a", title="Child A", parent_id=root.id, order=1
        )
        child_b = db.TextCollection(
            slug="child-b", title="Child B", parent_id=root.id, order=2
        )
        session.add_all([child_a, child_b])
        session.flush()

        grandchild = db.TextCollection(
            slug="grandchild", title="Grandchild", parent_id=child_a.id, order=1
        )
        session.add(grandchild)
        session.commit()

        ids = {
            "root": root.id,
            "child_a": child_a.id,
            "child_b": child_b.id,
            "grandchild": grandchild.id,
        }
        yield ids

        # Cleanup
        for cid in [ids["grandchild"], ids["child_a"], ids["child_b"], ids["root"]]:
            obj = session.get(db.TextCollection, cid)
            if obj:
                session.delete(obj)
        session.commit()


# --- Query helpers ---


def test_group_collections_by_parent(flask_app, collections):
    with flask_app.app_context():
        session = get_session()
        all_colls = session.query(db.TextCollection).all()
        by_parent = group_collections_by_parent(all_colls)

        root_ids = {c.id for c in by_parent.get(None, [])}
        assert collections["root"] in root_ids

        child_ids = {c.id for c in by_parent.get(collections["root"], [])}
        assert collections["child_a"] in child_ids
        assert collections["child_b"] in child_ids


def test_all_descendant_ids(flask_app, collections):
    with flask_app.app_context():
        session = get_session()
        all_colls = list(session.query(db.TextCollection).all())
        desc = all_descendant_ids(collections["root"], all_colls)

        assert set(desc) == {
            collections["root"],
            collections["child_a"],
            collections["child_b"],
            collections["grandchild"],
        }


def test_all_descendant_ids_leaf(flask_app, collections):
    with flask_app.app_context():
        session = get_session()
        all_colls = list(session.query(db.TextCollection).all())
        desc = all_descendant_ids(collections["grandchild"], all_colls)
        assert desc == [collections["grandchild"]]


# --- Admin routes ---


def test_admin_manage_collections(flask_app, admin_client, collections):
    resp = admin_client.get("/admin/collections")
    assert resp.status_code == 200


def test_admin_manage_collections_requires_admin(flask_app, rama_client):
    resp = rama_client.get("/admin/collections")
    assert resp.status_code == 404


def test_admin_create_collection(flask_app, admin_client):
    with flask_app.app_context():
        session = get_session()
        resp = admin_client.post(
            "/admin/collections/create",
            json={"slug": "test-new", "title": "Test New"},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert "id" in data

        # Cleanup
        coll = session.get(db.TextCollection, data["id"])
        if coll:
            session.delete(coll)
            session.commit()


def test_admin_create_collection_duplicate_slug(flask_app, admin_client, collections):
    resp = admin_client.post(
        "/admin/collections/create",
        json={"slug": "root", "title": "Dupe"},
    )
    assert resp.status_code == 400
    assert resp.get_json()["ok"] is False


def test_admin_edit_collection(flask_app, admin_client, collections):
    cid = collections["child_b"]
    resp = admin_client.patch(
        f"/admin/collections/{cid}",
        json={"title": "Child B Edited"},
    )
    assert resp.status_code == 200
    assert resp.get_json()["ok"] is True

    with flask_app.app_context():
        session = get_session()
        coll = session.get(db.TextCollection, cid)
        assert coll.title == "Child B Edited"
        # Restore
        coll.title = "Child B"
        session.commit()


def test_admin_edit_collection_not_found(flask_app, admin_client):
    resp = admin_client.patch(
        "/admin/collections/99999",
        json={"title": "Nope"},
    )
    assert resp.status_code == 404


def test_admin_delete_collection_reparents_children(
    flask_app, admin_client, collections
):
    """Deleting child_a should reparent grandchild to root."""
    with flask_app.app_context():
        session = get_session()

        resp = admin_client.delete(f"/admin/collections/{collections['child_a']}")
        assert resp.status_code == 200
        assert resp.get_json()["ok"] is True

        session.expire_all()
        gc = session.get(db.TextCollection, collections["grandchild"])
        assert gc is not None
        assert gc.parent_id == collections["root"]

        # child_a should be gone
        assert session.get(db.TextCollection, collections["child_a"]) is None


def test_admin_delete_collection_not_found(flask_app, admin_client):
    resp = admin_client.delete("/admin/collections/99999")
    assert resp.status_code == 404


def test_admin_save_tree(flask_app, admin_client, collections):
    items = [
        {"id": collections["child_a"], "parent_id": None, "order": 10},
        {"id": collections["child_b"], "parent_id": collections["child_a"], "order": 1},
    ]
    resp = admin_client.post(
        "/admin/collections/save-tree",
        json={"items": items},
    )
    assert resp.status_code == 200
    assert resp.get_json()["ok"] is True

    with flask_app.app_context():
        session = get_session()
        a = session.get(db.TextCollection, collections["child_a"])
        b = session.get(db.TextCollection, collections["child_b"])
        assert a.parent_id is None
        assert a.order == 10
        assert b.parent_id == collections["child_a"]
        assert b.order == 1

        # Restore
        a.parent_id = collections["root"]
        a.order = 1
        b.parent_id = collections["root"]
        b.order = 2
        session.commit()
