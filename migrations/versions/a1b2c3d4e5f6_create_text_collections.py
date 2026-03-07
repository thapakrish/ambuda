"""Create text collections

Revision ID: a1b2c3d4e5f6
Revises: d4e5f6a7b8c9
Create Date: 2026-03-07 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "text_collections",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "parent_id",
            sa.Integer,
            sa.ForeignKey("text_collections.id"),
            nullable=True,
            index=True,
        ),
        sa.Column("slug", sa.String, unique=True, nullable=False),
        sa.Column("order", sa.Integer, nullable=False, default=0),
        sa.Column("title", sa.String, nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
    )
    op.create_table(
        "text_collection_association",
        sa.Column(
            "text_id",
            sa.Integer,
            sa.ForeignKey("texts.id"),
            primary_key=True,
        ),
        sa.Column(
            "collection_id",
            sa.Integer,
            sa.ForeignKey("text_collections.id"),
            primary_key=True,
        ),
    )


def downgrade() -> None:
    op.drop_table("text_collection_association")
    op.drop_table("text_collections")
