from fastapi import HTTPException
from sqlalchemy import select, func, and_

from api.marketplace.service.view_event_service.schemas import CreateViewEventRequest, CreateViewEventResponse, \
    GetViewEventsRequest, GetViewEventsList, ViewEvent

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from database.db import marketplace_view_events, database, warehouses, nomenclature


class MarketplaceViewEventService(BaseMarketplaceService):
    async def create_view_event(self, request: CreateViewEventRequest) -> CreateViewEventResponse:
        contragent_id = await self.__get_contragent_id_by_phone(request.contragent_phone)

        if request.entity_type == 'warehouse':
            cashbox_id = select(warehouses.c.cashbox).where(warehouses.c.id == request.entity_id)
        elif request.entity_type == 'nomenclature':
            cashbox_id = select(nomenclature.c.cashbox).where(nomenclature.c.id == request.entity_id)
        else:
            raise HTTPException(status_code=422, detail='Неизвестный entity_type')

        query = marketplace_view_events.insert().values(
            cashbox_id=cashbox_id,
            entity_type=request.entity_type,
            entity_id=request.entity_id,
            listing_pos=request.listing_pos,
            listing_page=request.listing_page,
            contragent_id=contragent_id,
        )
        await database.execute(query)
        return CreateViewEventResponse(success=True, message="Событие просмотра успешно сохранено")

    async def get_view_events(self, request: GetViewEventsRequest) -> GetViewEventsList:
        query = select(marketplace_view_events).where()
        conditions = [marketplace_view_events.c.cashbox_id == request.cashbox_id]

        if request.entity_type:
            conditions.append(marketplace_view_events.c.entity_type == request.entity_type)
        if request.contragent_phone:
            contragent_id = await self.__get_contragent_id_by_phone(request.contragent_phone)
            conditions.append(marketplace_view_events.c.contragent_id == contragent_id)
        if request.from_time:
            conditions.append(marketplace_view_events.c.created_at >= request.from_time)
        if request.to_time:
            conditions.append(marketplace_view_events.c.created_at <= request.to_time)

        query = query.where(and_(*conditions))
        count_query = select(func.count(marketplace_view_events.c.id)).where(and_(*conditions))

        result = await database.fetch_all(query)
        count_result = await database.fetch_val(count_query)
        return GetViewEventsList(
            events=[ViewEvent.from_orm(i) for i in result],
            count=count_result,
        )
