"""Add cascade deletes to text_collection_association

Revision ID: d1e2f3a4b5c6
Revises: c1d2e3f4a5b6
Create Date: 2026-03-07 00:00:03.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d1e2f3a4b5c6"
down_revision = "c1d2e3f4a5b6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # SQLite doesn't support ALTER CONSTRAINT, so we recreate the table
    # with the desired FK constraints via batch mode.
    conn = op.get_bind()
    # Copy data, drop old table, create new with CASCADE, copy data back
    conn.execute(
        sa.text(
            "CREATE TABLE _tca_new ("
            "  text_id INTEGER NOT NULL REFERENCES texts(id) ON DELETE CASCADE,"
            "  collection_id INTEGER NOT NULL REFERENCES text_collections(id) ON DELETE CASCADE,"
            "  PRIMARY KEY (text_id, collection_id)"
            ")"
        )
    )
    conn.execute(
        sa.text(
            "INSERT INTO _tca_new SELECT text_id, collection_id FROM text_collection_association"
        )
    )
    conn.execute(sa.text("DROP TABLE text_collection_association"))
    conn.execute(sa.text("ALTER TABLE _tca_new RENAME TO text_collection_association"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text(
            "CREATE TABLE _tca_new ("
            "  text_id INTEGER NOT NULL REFERENCES texts(id),"
            "  collection_id INTEGER NOT NULL REFERENCES text_collections(id),"
            "  PRIMARY KEY (text_id, collection_id)"
            ")"
        )
    )
    conn.execute(
        sa.text(
            "INSERT INTO _tca_new SELECT text_id, collection_id FROM text_collection_association"
        )
    )
    conn.execute(sa.text("DROP TABLE text_collection_association"))
    conn.execute(sa.text("ALTER TABLE _tca_new RENAME TO text_collection_association"))
