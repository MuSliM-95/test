from fastapi import HTTPException
from sqlalchemy import select, func, and_

from api.marketplace.service.view_event_service.schemas import CreateViewEventRequest, CreateViewEventResponse, \
    GetViewEventsRequest, GetViewEventsList, ViewEvent, ViewEventsUtm

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from database.db import marketplace_view_events, database, warehouses, nomenclature


class MarketplaceViewEventService(BaseMarketplaceService):
    async def create_view_event(self, request: CreateViewEventRequest, utm: ViewEventsUtm) -> CreateViewEventResponse:
        contragent_id = await self._get_contragent_id_by_phone(request.contragent_phone)

        if request.entity_type == 'warehouse':
            cashbox_id = await database.fetch_val(
                select(warehouses.c.cashbox).where(warehouses.c.id == request.entity_id)
            )
        elif request.entity_type == 'nomenclature':
            cashbox_id = await database.fetch_val(
                select(nomenclature.c.cashbox).where(nomenclature.c.id == request.entity_id)
            )
        else:
            raise HTTPException(status_code=422, detail='Неизвестный entity_type')

        if cashbox_id is None:
            raise HTTPException(
                status_code=404,
                detail=f'{request.entity_type} с ID {request.entity_id} не найден'
            )

        query = marketplace_view_events.insert().values(
            cashbox_id=cashbox_id,
            entity_type=request.entity_type,
            entity_id=request.entity_id,
            listing_pos=request.listing_pos,
            listing_page=request.listing_page,
            event=request.event.value,
            contragent_id=contragent_id,
        )
        view_event_id = await database.execute(query)

        # добавляем utm
        await self._add_utm(view_event_id, utm)

        return CreateViewEventResponse(success=True, message="Событие просмотра успешно сохранено")

    async def get_view_events(self, request: GetViewEventsRequest) -> GetViewEventsList:
        conditions = [marketplace_view_events.c.cashbox_id == request.cashbox_id]

        if request.entity_type:
            conditions.append(marketplace_view_events.c.entity_type == str(request.entity_type))
        if request.event:
            conditions.append(marketplace_view_events.c.event == request.event.value)
        if request.contragent_phone:
            contragent_id = await self._get_contragent_id_by_phone(request.contragent_phone)
            conditions.append(marketplace_view_events.c.contragent_id == contragent_id)
        if request.from_time:
            conditions.append(marketplace_view_events.c.created_at >= request.from_time)
        if request.to_time:
            conditions.append(marketplace_view_events.c.created_at <= request.to_time)

        query = select(marketplace_view_events).where(and_(*conditions))
        count_query = select(func.count(marketplace_view_events.c.id)).where(and_(*conditions))

        result = await database.fetch_all(query)
        count_result = await database.fetch_val(count_query)
        return GetViewEventsList(
            events=[ViewEvent.from_orm(i) for i in result],
            count=count_result,
        )
