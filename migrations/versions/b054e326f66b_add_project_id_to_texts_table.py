"""Add project_id to texts table

Revision ID: b054e326f66b
Revises: 3f4bacd1d993
Create Date: 2025-12-07 11:52:43.719188

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "b054e326f66b"
down_revision = "3f4bacd1d993"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("texts", sa.Column("project_id", sa.Integer(), nullable=True))
    op.create_index(op.f("ix_texts_project_id"), "texts", ["project_id"], unique=False)
    with op.batch_alter_table("texts") as batch_op:
        batch_op.create_foreign_key(
            "fk_texts_project_id", "proof_projects", ["project_id"], ["id"]
        )


def downgrade() -> None:
    with op.batch_alter_table("texts") as batch_op:
        batch_op.drop_constraint("fk_texts_project_id", type_="foreignkey")
    op.drop_index(op.f("ix_texts_project_id"), table_name="texts")
    op.drop_column("texts", "project_id")
