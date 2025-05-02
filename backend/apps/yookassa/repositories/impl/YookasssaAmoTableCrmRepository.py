from sqlalchemy import select

from apps.yookassa.repositories.core.IYookasssaAmoTableCrmRepository import IYookasssaAmoTableCrmRepository
from database.db import database, amo_install_table_cashboxes, amo_install


class YookasssaAmoTableCrmRepository(IYookasssaAmoTableCrmRepository):
    async def get_active_install_by_cashbox(self, cashbox: int):
        return await database.fetch_one(
            select(
                amo_install_table_cashboxes.c.cashbox_id,
                amo_install_table_cashboxes.c.amo_install_group_id,
            ).where(
                amo_install_table_cashboxes.c.cashbox_id == cashbox
            ).select_from(
                amo_install_table_cashboxes
            ).join(
                amo_install,
                amo_install_table_cashboxes.c.amo_install_group_id == amo_install.c.install_group_id
            ).where(
                amo_install.c.active == True
            )
        )

