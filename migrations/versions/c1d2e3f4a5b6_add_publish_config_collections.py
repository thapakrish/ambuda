"""Add publish_config_collection_association table and drop genre from publish_configs

Revision ID: c1d2e3f4a5b6
Revises: b1c2d3e4f5a6
Create Date: 2026-03-07 00:00:02.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c1d2e3f4a5b6"
down_revision = "b1c2d3e4f5a6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    tables = sa.inspect(conn).get_table_names()
    if "publish_config_collection_association" not in tables:
        op.create_table(
            "publish_config_collection_association",
            sa.Column(
                "publish_config_id",
                sa.Integer,
                sa.ForeignKey("publish_configs.id", ondelete="CASCADE"),
                primary_key=True,
            ),
            sa.Column(
                "collection_id",
                sa.Integer,
                sa.ForeignKey("text_collections.id", ondelete="CASCADE"),
                primary_key=True,
            ),
        )
    columns = [c["name"] for c in sa.inspect(conn).get_columns("publish_configs")]
    if "genre" in columns:
        with op.batch_alter_table("publish_configs") as batch_op:
            batch_op.drop_column("genre")


def downgrade() -> None:
    with op.batch_alter_table("publish_configs") as batch_op:
        batch_op.add_column(sa.Column("genre", sa.String, nullable=True))
    op.drop_table("publish_config_collection_association")
