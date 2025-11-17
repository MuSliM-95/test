import math
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
    async def _validate_contragent(contragent_phone: str, nomenclature_id: int):
        try:
            contragent_query = select(contragents.c.cashbox).where(contragents.c.phone == contragent_phone)
            nomenclature_query = select(nomenclature.c.cashbox).where(nomenclature.c.id == nomenclature_id)
            if not ((await database.fetch_one(contragent_query)).cashbox == (await database.fetch_one(nomenclature_query)).cashbox):
                raise HTTPException(status_code=422, detail='Контрагент не принадлежит этому кешбоксу')
        except AttributeError:
            raise HTTPException(status_code=404, detail="Контрагент или номенклатура с таким номером телефона не найден")

    @staticmethod
    async def _add_utm(entity_id: int, utm: BaseMarketplaceUtm) -> int:
        query = marketplace_utm_tags.insert().values(
            entity_id=entity_id,
            entity_type=utm.entity_type.value,
            **utm.dict(exclude={'entity_type'}),
        )
        res = await database.execute(query)
        return res

    @staticmethod
    def _count_distance_to_client(client_lat: Optional[float], client_long: Optional[float], warehouse_lat: Optional[float], warehouse_long: Optional[float]) -> Optional[float]:
        if not all([client_lat, client_long, warehouse_lat, warehouse_long]):
            return None

        R = 6371.0  # радиус Земли в километрах

        lat1_rad = math.radians(client_lat)
        lon1_rad = math.radians(client_long)
        lat2_rad = math.radians(warehouse_lat)
        lon2_rad = math.radians(warehouse_long)

        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        distance = R * c
        return distance  # в километрах

    # @staticmethod
    # async def _hash_order(contragent_id: ):