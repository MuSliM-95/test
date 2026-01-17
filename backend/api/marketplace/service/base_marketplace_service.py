import math
import time
from typing import List, Optional

from fastapi import HTTPException
from sqlalchemy import and_, desc, select
from sqlalchemy.dialects.postgresql import insert

from fastapi import HTTPException
from sqlalchemy import select

from api.marketplace.schemas import BaseMarketplaceUtm
from api.marketplace.service.products_list_service.schemas import AvailableWarehouse
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from database.db import (
    contragents,
    database,
    marketplace_clients_list,
    marketplace_utm_tags,
    nomenclature,
    warehouse_balances,
    warehouses,
)


class BaseMarketplaceService:
    def __init__(self):
        self._rabbitmq: Optional[IRabbitMessaging] = None
        self._entity_types_to_tables = {
            "nomenclature": nomenclature,
            "warehouses": warehouses,
        }

    @staticmethod
    async def _get_contragent_id_by_phone(phone: str) -> int:
        try:
            contragent_query = select(contragents.c.id).where(
                contragents.c.phone == phone
            )
            row = await database.fetch_one(contragent_query)
            return row.id
        except AttributeError:
            raise HTTPException(
                status_code=404,
                detail="Контрагент с таким номером телефона не найден",
            )

    @staticmethod
    async def _get_or_create_contragent_id(phone: str, cashbox_id: int) -> int:
        if not phone:
            raise HTTPException(status_code=422, detail="Не указан номер телефона")

        q = select(contragents.c.id).where(
            and_(
                contragents.c.phone == phone,
                contragents.c.cashbox == cashbox_id,
                contragents.c.is_deleted.is_not(True),
            )
        )
        row = await database.fetch_one(q)
        if row:
            return row.id

        now_ts = int(time.time())
        ins = (
            contragents.insert()
            .values(
                phone=phone,
                name=phone,
                cashbox=cashbox_id,
                is_deleted=False,
                is_phone_formatted=False,
                created_at=now_ts,
                updated_at=now_ts,
            )
            .returning(contragents.c.id)
        )

        created = await database.fetch_one(ins)
        if not created:
            raise HTTPException(
                status_code=500,
                detail="Не удалось создать контрагента для оформления заказа",
            )
        return created.id

    @staticmethod
    async def _validate_contragent(contragent_phone: str, nomenclature_id: int) -> None:
        try:
            contragent_query = select(contragents.c.cashbox).where(
                contragents.c.phone == contragent_phone
            )
            nomenclature_query = select(nomenclature.c.cashbox).where(
                nomenclature.c.id == nomenclature_id
            )
            if not (
                (await database.fetch_one(contragent_query)).cashbox
                == (await database.fetch_one(nomenclature_query)).cashbox
            ):
                raise HTTPException(
                    status_code=422, detail="Контрагент не принадлежит этому кешбоксу"
                )
        except AttributeError:
            raise HTTPException(
                status_code=404,
                detail="Контрагент или номенклатура с таким номером телефона не найден",
            )

    @staticmethod
    async def _add_utm(entity_id: int, utm: BaseMarketplaceUtm) -> int:
        query = marketplace_utm_tags.insert().values(
            entity_id=entity_id,
            entity_type=utm.entity_type.value,
            **utm.dict(exclude={"entity_type"}),
        )
        res = await database.execute(query)
        return res

    @staticmethod
    def _count_distance_to_client(
        client_lat: Optional[float],
        client_long: Optional[float],
        warehouse_lat: Optional[float],
        warehouse_long: Optional[float],
    ) -> Optional[float]:
        if not all([client_lat, client_long, warehouse_lat, warehouse_long]):
            return None

        R = 6371.0  # радиус Земли в километрах

        lat1_rad = math.radians(client_lat)
        lon1_rad = math.radians(client_long)
        lat2_rad = math.radians(warehouse_lat)
        lon2_rad = math.radians(warehouse_long)

        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        distance = R * c
        return distance  # в километрах

    @staticmethod
    async def _ensure_marketplace_client(phone: str) -> None:
        if not phone:
            raise HTTPException(status_code=422, detail="Не указан номер телефона")

        stmt = (
            insert(marketplace_clients_list)
            .values(phone=phone)
            .on_conflict_do_nothing(index_elements=["phone"])
        )
        await database.execute(stmt)

    @staticmethod
    async def _fetch_available_warehouses(
        nomenclature_id: int,
        client_lat: Optional[float] = None,
        client_lon: Optional[float] = None,
        limit: int = 50,
    ) -> List[AvailableWarehouse]:
        from sqlalchemy import func

        wb_ranked = (
            select(
                warehouse_balances.c.organization_id.label("organization_id"),
                warehouse_balances.c.warehouse_id.label("warehouse_id"),
                warehouse_balances.c.nomenclature_id.label("nomenclature_id"),
                warehouse_balances.c.current_amount.label("current_amount"),
                warehouses.c.name.label("warehouse_name"),
                warehouses.c.address.label("warehouse_address"),
                warehouses.c.latitude.label("latitude"),
                warehouses.c.longitude.label("longitude"),
                func.row_number()
                .over(
                    partition_by=[
                        warehouse_balances.c.organization_id,
                        warehouse_balances.c.warehouse_id,
                        warehouse_balances.c.nomenclature_id,
                    ],
                    order_by=[
                        desc(warehouse_balances.c.created_at),
                        desc(warehouse_balances.c.id),
                    ],
                )
                .label("rn"),
            )
            .select_from(
                warehouse_balances.join(
                    warehouses,
                    and_(
                        warehouses.c.id == warehouse_balances.c.warehouse_id,
                        warehouses.c.is_public.is_(True),
                        warehouses.c.status.is_(True),
                        warehouses.c.is_deleted.is_not(True),
                    ),
                )
            )
            .where(warehouse_balances.c.nomenclature_id == nomenclature_id)
            .subquery()
        )

        query = (
            select(
                wb_ranked.c.warehouse_id,
                wb_ranked.c.organization_id,
                wb_ranked.c.current_amount,
                wb_ranked.c.warehouse_name,
                wb_ranked.c.warehouse_address,
                wb_ranked.c.latitude,
                wb_ranked.c.longitude,
            )
            .where(
                and_(
                    wb_ranked.c.rn == 1,
                    wb_ranked.c.current_amount > 0,
                )
            )
            .limit(limit)
        )

        rows = await database.fetch_all(query)
        if not rows:
            return []

        result: List[AvailableWarehouse] = []
        for r in rows:
            d = dict(r)
            result.append(
                AvailableWarehouse(
                    warehouse_id=d["warehouse_id"],
                    organization_id=d["organization_id"],
                    warehouse_name=d.get("warehouse_name"),
                    warehouse_address=d.get("warehouse_address"),
                    latitude=d.get("latitude"),
                    longitude=d.get("longitude"),
                    current_amount=d.get("current_amount"),
                    distance_to_client=BaseMarketplaceService._count_distance_to_client(
                        client_lat, client_lon, d.get("latitude"), d.get("longitude")
                    ),
                )
            )

        # Сортировка:
        # - если есть координаты клиента: сначала по расстоянию
        # - иначе: по остатку (DESC) как более практичный дефолт
        if client_lat is not None and client_lon is not None:
            result.sort(
                key=lambda x: (x.distance_to_client is None, x.distance_to_client or 0)
            )
        else:
            result.sort(key=lambda x: -(x.current_amount or 0))

        return result

    @staticmethod
    async def _get_latest_organization_id_for_balance(
        warehouse_id: int, nomenclature_id: int
    ) -> int:
        """Найти organization_id по warehouse+номенклатура (по последнему балансу)."""
        q = (
            select(warehouse_balances.c.organization_id)
            .where(
                and_(
                    warehouse_balances.c.warehouse_id == warehouse_id,
                    warehouse_balances.c.nomenclature_id == nomenclature_id,
                )
            )
            .order_by(
                desc(warehouse_balances.c.created_at), desc(warehouse_balances.c.id)
            )
            .limit(1)
        )
        row = await database.fetch_one(q)
        if not row:
            raise HTTPException(
                status_code=404,
                detail="Не удалось определить организацию для выбранного склада/товара",
            )
        return row.organization_id
