"""Add status to TokenBlock and TokenRevision

Revision ID: f0165b2bee3d
Revises: 6df22e09f833
Create Date: 2025-12-17 22:46:05.168585

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "f0165b2bee3d"
down_revision = "6df22e09f833"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("token_blocks", schema=None) as batch_op:
        batch_op.add_column(sa.Column("status", sa.String(), nullable=True))

    with op.batch_alter_table("token_revisions", schema=None) as batch_op:
        batch_op.add_column(sa.Column("status", sa.String(), nullable=True))

    op.execute("UPDATE token_blocks SET status = 'r0' WHERE status IS NULL")
    op.execute("UPDATE token_revisions SET status = 'r0' WHERE status IS NULL")

    with op.batch_alter_table("token_blocks", schema=None) as batch_op:
        batch_op.alter_column("status", nullable=False)

    with op.batch_alter_table("token_revisions", schema=None) as batch_op:
        batch_op.alter_column("status", nullable=False)


def downgrade() -> None:
    with op.batch_alter_table("token_revisions", schema=None) as batch_op:
        batch_op.drop_column("status")

    with op.batch_alter_table("token_blocks", schema=None) as batch_op:
        batch_op.drop_column("status")
