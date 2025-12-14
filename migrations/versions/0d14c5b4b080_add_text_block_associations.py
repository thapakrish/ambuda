"""Add text block associations

Revision ID: 0d14c5b4b080
Revises: c6c18d3bdec1
Create Date: 2025-12-14 11:31:56.638498

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "0d14c5b4b080"
down_revision = "c6c18d3bdec1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "text_block_associations",
        sa.Column("parent_id", sa.Integer(), nullable=False),
        sa.Column("child_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["child_id"],
            ["text_blocks.id"],
        ),
        sa.ForeignKeyConstraint(
            ["parent_id"],
            ["text_blocks.id"],
        ),
        sa.PrimaryKeyConstraint("parent_id", "child_id"),
    )
    with op.batch_alter_table("texts", schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f("ix_texts_parent_id"), ["parent_id"], unique=False
        )


def downgrade() -> None:
    with op.batch_alter_table("texts", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_texts_parent_id"))

    op.drop_table("text_block_associations")
