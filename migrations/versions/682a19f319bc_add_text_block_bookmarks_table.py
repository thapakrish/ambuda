"""Add text_block_bookmarks table

Revision ID: 682a19f319bc
Revises: 87f1671485a9
Create Date: 2026-01-18 14:16:20.391555

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "682a19f319bc"
down_revision = "87f1671485a9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create text_block_bookmarks table with composite primary key
    op.create_table(
        "text_block_bookmarks",
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("block_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["block_id"],
            ["text_blocks.id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
        ),
        sa.PrimaryKeyConstraint("user_id", "block_id"),
    )


def downgrade() -> None:
    # Drop the text_block_bookmarks table
    op.drop_table("text_block_bookmarks")
