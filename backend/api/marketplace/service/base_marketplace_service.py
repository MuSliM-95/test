from typing import Optional

from fastapi import HTTPException
from sqlalchemy import select

from api.marketplace.schemas import BaseMarketplaceUtm
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from database.db import nomenclature, warehouses, contragents, database, marketplace_utm_tags


class BaseMarketplaceService:
    def __init__(self):
        self._rabbitmq: Optional[IRabbitMessaging] = None
        self._entity_types_to_tables = {
            'nomenclature': nomenclature,
            'warehouses': warehouses,
        }

    @staticmethod
    async def _get_contragent_id_by_phone(phone: str):
        try:
            contragent_query = select(contragents.c.id).where(contragents.c.phone == phone)
            return (await database.fetch_one(contragent_query)).id
        except AttributeError:
            raise HTTPException(status_code=404, detail="Контрагент с таким номером телефона не найден")

    @staticmethod
    async def _add_utm(entity_id: int, utm: BaseMarketplaceUtm) -> int:
        query = marketplace_utm_tags.insert().values(
            entity_id=entity_id,
            entity_type=utm.entity_type.value,
            **utm.dict(exclude={'entity_type'}),
        )
        res = await database.execute(query)
        return res
