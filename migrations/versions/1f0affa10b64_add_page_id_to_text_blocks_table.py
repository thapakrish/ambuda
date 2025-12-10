"""Add page_id to text_blocks table

Revision ID: 1f0affa10b64
Revises: b054e326f66b
Create Date: 2025-12-07 12:28:19.022588

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "1f0affa10b64"
down_revision = "b054e326f66b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("text_blocks", sa.Column("page_id", sa.Integer(), nullable=True))
    op.create_index(
        op.f("ix_text_blocks_page_id"), "text_blocks", ["page_id"], unique=False
    )
    with op.batch_alter_table("text_blocks") as batch_op:
        batch_op.create_foreign_key(
            "fk_text_blocks_page_id", "proof_pages", ["page_id"], ["id"]
        )


def downgrade() -> None:
    with op.batch_alter_table("text_blocks") as batch_op:
        batch_op.drop_constraint("fk_text_blocks_page_id", type_="foreignkey")
    op.drop_index(op.f("ix_text_blocks_page_id"), table_name="text_blocks")
    op.drop_column("text_blocks", "page_id")
