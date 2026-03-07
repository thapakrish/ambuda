"""Drop text_tags and text_tag_association tables

Revision ID: b1c2d3e4f5a6
Revises: a1b2c3d4e5f6
Create Date: 2026-03-07 00:00:01.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b1c2d3e4f5a6"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("text_tag_association")
    op.drop_table("text_tags")


def downgrade() -> None:
    op.create_table(
        "text_tags",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String, unique=True, nullable=False),
        sa.Column("description", sa.Text, nullable=True),
    )
    op.create_table(
        "text_tag_association",
        sa.Column(
            "text_id",
            sa.Integer,
            sa.ForeignKey("texts.id"),
            primary_key=True,
        ),
        sa.Column(
            "tag_id",
            sa.Integer,
            sa.ForeignKey("text_tags.id"),
            primary_key=True,
        ),
        sa.Column("is_featured", sa.Boolean, default=False, nullable=False),
    )
