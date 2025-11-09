from typing import Optional

from fastapi import HTTPException
from sqlalchemy import select

from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from database.db import nomenclature, warehouses, contragents, database


class BaseMarketplaceService:
    def __init__(self):
        self.__rabbitmq: Optional[IRabbitMessaging] = None
        self.__entity_types_to_tables = {
            'nomenclature': nomenclature,
            'warehouses': warehouses,
        }

    @staticmethod
    async def __get_contragent_id_by_phone(phone: str):
        try:
            contragent_query = select(contragents.c.id).where(contragents.c.phone == phone)
            return (await database.fetch_one(contragent_query)).id
        except AttributeError:
            raise HTTPException(status_code=404, detail="Контрагент с таким номером телефона не найден")
