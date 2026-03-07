"""Tests for two-way sync between PublishConfig and Text collections.

Covers:
- Saving collection_ids on PublishConfig
- Publish sync: config.collections → text.collections
- Batch edit sync: text.collections → config.collections
- CASCADE deletes on association tables
"""

import pytest

import ambuda.database as db
from ambuda.queries import get_session


@pytest.fixture()
def sync_env(flask_app):
    """Create collections, a text, and a PublishConfig linked to the test project."""
    with flask_app.app_context():
        session = get_session()

        # Collections
        coll_a = db.TextCollection(slug="sync-a", title="Sync A", order=1)
        coll_b = db.TextCollection(slug="sync-b", title="Sync B", order=2)
        session.add_all([coll_a, coll_b])
        session.flush()

        # Use existing project (from conftest)
        project = session.query(db.Project).filter_by(slug="test-project").one()

        # A text for sync testing
        text = db.Text(slug="sync-test-text", title="Sync Test")
        session.add(text)
        session.flush()

        # PublishConfig linked to project and text
        pc = db.PublishConfig(
            project_id=project.id,
            text_id=text.id,
            slug="sync-test-text",
            title="Sync Test",
            order=0,
        )
        session.add(pc)
        session.commit()

        env = {
            "coll_a_id": coll_a.id,
            "coll_b_id": coll_b.id,
            "text_id": text.id,
            "pc_id": pc.id,
            "project_id": project.id,
        }
        yield env

        # Cleanup
        session.query(db.PublishConfig).filter_by(id=env["pc_id"]).delete()
        text_obj = session.get(db.Text, env["text_id"])
        if text_obj:
            # Remove sections/blocks first if any
            session.query(db.TextSection).filter_by(text_id=text_obj.id).delete()
            text_obj.collections = []
            session.delete(text_obj)
        for cid in [env["coll_a_id"], env["coll_b_id"]]:
            c = session.get(db.TextCollection, cid)
            if c:
                session.delete(c)
        session.commit()


class TestConfigCollections:
    """Test saving/loading collection_ids on PublishConfig."""

    def test_config_save_stores_collection_ids(self, flask_app, sync_env):
        with flask_app.app_context():
            session = get_session()
            pc = session.get(db.PublishConfig, sync_env["pc_id"])
            coll_a = session.get(db.TextCollection, sync_env["coll_a_id"])
            coll_b = session.get(db.TextCollection, sync_env["coll_b_id"])

            pc.collections = [coll_a, coll_b]
            session.commit()

            # Re-fetch to verify persistence
            pc = session.get(db.PublishConfig, sync_env["pc_id"])
            coll_ids = {c.id for c in pc.collections}
            assert coll_ids == {sync_env["coll_a_id"], sync_env["coll_b_id"]}

            # Cleanup
            pc.collections = []
            session.commit()

    def test_config_save_clears_collections(self, flask_app, sync_env):
        with flask_app.app_context():
            session = get_session()
            pc = session.get(db.PublishConfig, sync_env["pc_id"])
            coll_a = session.get(db.TextCollection, sync_env["coll_a_id"])

            pc.collections = [coll_a]
            session.commit()

            pc.collections = []
            session.commit()

            pc = session.get(db.PublishConfig, sync_env["pc_id"])
            assert pc.collections == []


class TestPublishSync:
    """Test that publishing syncs config.collections → text.collections."""

    def test_publish_syncs_collections_to_text(self, flask_app, sync_env):
        """Simulate the sync that happens in publish.create()."""
        with flask_app.app_context():
            session = get_session()
            pc = session.get(db.PublishConfig, sync_env["pc_id"])
            text = session.get(db.Text, sync_env["text_id"])
            coll_a = session.get(db.TextCollection, sync_env["coll_a_id"])
            coll_b = session.get(db.TextCollection, sync_env["coll_b_id"])

            # Set collections on config
            pc.collections = [coll_a, coll_b]
            session.flush()

            # Simulate publish sync: text.collections = list(config.collections)
            text.collections = list(pc.collections)
            session.commit()

            text = session.get(db.Text, sync_env["text_id"])
            text_coll_ids = {c.id for c in text.collections}
            assert text_coll_ids == {sync_env["coll_a_id"], sync_env["coll_b_id"]}

            # Cleanup
            text.collections = []
            pc.collections = []
            session.commit()


class TestBatchEditSync:
    """Test batch edit: text.collections → config.collections."""

    def test_batch_add_collection_syncs_to_config(self, flask_app, sync_env):
        with flask_app.app_context():
            session = get_session()
            text = session.get(db.Text, sync_env["text_id"])
            pc = session.get(db.PublishConfig, sync_env["pc_id"])
            coll_a = session.get(db.TextCollection, sync_env["coll_a_id"])

            # Simulate batch add
            if coll_a not in text.collections:
                text.collections.append(coll_a)

            # Sync text → config (as in batch_edit_collections view)
            configs = (
                session.query(db.PublishConfig)
                .filter(db.PublishConfig.text_id == text.id)
                .all()
            )
            for config in configs:
                config.collections = list(text.collections)
            session.commit()

            pc = session.get(db.PublishConfig, sync_env["pc_id"])
            assert {c.id for c in pc.collections} == {sync_env["coll_a_id"]}

            # Cleanup
            text.collections = []
            pc.collections = []
            session.commit()

    def test_batch_remove_collection_syncs_to_config(self, flask_app, sync_env):
        with flask_app.app_context():
            session = get_session()
            text = session.get(db.Text, sync_env["text_id"])
            pc = session.get(db.PublishConfig, sync_env["pc_id"])
            coll_a = session.get(db.TextCollection, sync_env["coll_a_id"])
            coll_b = session.get(db.TextCollection, sync_env["coll_b_id"])

            # Setup: text and config both have [a, b]
            text.collections = [coll_a, coll_b]
            pc.collections = [coll_a, coll_b]
            session.commit()

            # Simulate batch remove of coll_b
            text.collections.remove(coll_b)

            # Sync
            configs = (
                session.query(db.PublishConfig)
                .filter(db.PublishConfig.text_id == text.id)
                .all()
            )
            for config in configs:
                config.collections = list(text.collections)
            session.commit()

            pc = session.get(db.PublishConfig, sync_env["pc_id"])
            assert {c.id for c in pc.collections} == {sync_env["coll_a_id"]}

            # Cleanup
            text.collections = []
            pc.collections = []
            session.commit()

    def test_batch_edit_no_config(self, flask_app, sync_env):
        """If a text has no PublishConfig, batch edit should not error."""
        with flask_app.app_context():
            session = get_session()

            # Create a text with no PublishConfig
            orphan = db.Text(slug="orphan-text", title="Orphan")
            session.add(orphan)
            session.flush()

            coll_a = session.get(db.TextCollection, sync_env["coll_a_id"])
            orphan.collections.append(coll_a)

            # Sync (no configs exist)
            configs = (
                session.query(db.PublishConfig)
                .filter(db.PublishConfig.text_id == orphan.id)
                .all()
            )
            assert configs == []
            for config in configs:
                config.collections = list(orphan.collections)

            session.commit()

            # Cleanup
            orphan.collections = []
            session.delete(orphan)
            session.commit()


class TestCascadeDeletes:
    """Test that deleting a collection cascades to association tables."""

    def test_delete_collection_cascades_text_association(self, flask_app, sync_env):
        with flask_app.app_context():
            session = get_session()
            text = session.get(db.Text, sync_env["text_id"])
            coll_a = session.get(db.TextCollection, sync_env["coll_a_id"])

            text.collections = [coll_a]
            session.commit()

            # Delete the collection
            session.delete(coll_a)
            session.commit()

            # Text should have no collections
            text = session.get(db.Text, sync_env["text_id"])
            assert sync_env["coll_a_id"] not in {c.id for c in text.collections}

            # Recreate for cleanup fixture
            new_coll = db.TextCollection(
                id=sync_env["coll_a_id"], slug="sync-a", title="Sync A", order=1
            )
            session.add(new_coll)
            session.commit()

    def test_delete_collection_cascades_config_association(self, flask_app, sync_env):
        with flask_app.app_context():
            session = get_session()
            pc = session.get(db.PublishConfig, sync_env["pc_id"])
            coll_b = session.get(db.TextCollection, sync_env["coll_b_id"])

            pc.collections = [coll_b]
            session.commit()

            # Delete the collection
            session.delete(coll_b)
            session.commit()

            # Config should have no collections
            pc = session.get(db.PublishConfig, sync_env["pc_id"])
            assert sync_env["coll_b_id"] not in {c.id for c in pc.collections}

            # Recreate for cleanup fixture
            new_coll = db.TextCollection(
                id=sync_env["coll_b_id"], slug="sync-b", title="Sync B", order=2
            )
            session.add(new_coll)
            session.commit()
