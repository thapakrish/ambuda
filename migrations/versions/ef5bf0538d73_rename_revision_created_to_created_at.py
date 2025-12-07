"""rename_revision_created_to_created_at

Revision ID: ef5bf0538d73
Revises: 3c439e646d53
Create Date: 2025-11-30 22:24:08.018158

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "ef5bf0538d73"
down_revision = "3c439e646d53"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("proof_revisions") as batch_op:
        batch_op.alter_column("created", new_column_name="created_at")


def downgrade() -> None:
    with op.batch_alter_table("proof_revisions") as batch_op:
        batch_op.alter_column("created_at", new_column_name="created")
