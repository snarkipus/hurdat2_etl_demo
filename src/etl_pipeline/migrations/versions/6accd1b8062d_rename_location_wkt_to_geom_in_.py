"""Rename location_wkt to geom in observations table

Revision ID: 6accd1b8062d
Revises: 53dc9abc36a8
Create Date: 2025-03-29 13:55:02.172026

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6accd1b8062d'
down_revision: Union[str, None] = '53dc9abc36a8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('observations', schema=None) as batch_op:
        batch_op.alter_column('location_wkt', new_column_name='geom',
                              existing_type=sa.String(),
                              existing_nullable=False)


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('observations', schema=None) as batch_op:
        batch_op.alter_column('geom', new_column_name='location_wkt',
                              existing_type=sa.String(),
                              existing_nullable=False)
