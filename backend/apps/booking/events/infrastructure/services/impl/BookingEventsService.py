from typing import List

from sqlalchemy import and_, select

from database.db import pictures, database, booking_events, booking_nomenclature, booking, booking_events_photo
from ...repositories.core.IBookingEventsRepository import IBookingEventsRepository
from ..core.IBookingEventsService import IBookingEventsService
from ....domain.models.AddBookingEventPhotoModel import AddBookingEventPhotoModel
from ....domain.models.CreateBookingEventModel import CreateBookingEventModel
from ....domain.models.ReponseCreatedBookingEventModel import ResponseCreatedBookingEventModel


class BookingEventsService(IBookingEventsService):

    def __init__(
        self,
        booking_events_repository: IBookingEventsRepository
    ):
        self.__booking_events_repository = booking_events_repository

    async def add_one(self, events: CreateBookingEventModel) -> ResponseCreatedBookingEventModel:
        created_event_id = await self.__booking_events_repository.add_one(
            event=events
        )
        return ResponseCreatedBookingEventModel(
            id=created_event_id,
            **events.dict()
        )

    async def add_more(self, events: List[CreateBookingEventModel]):
        for_create_list = list(map(lambda row: {**row.dict(), "is_deleted": False}, events))
        created_event_ids = await self.__booking_events_repository.add_more(
            events=for_create_list
        )
        return [{"id": created_event_id, **created_event} for created_event, created_event_id in zip(for_create_list, created_event_ids)]

    async def add_photos(self, events_photo: List[AddBookingEventPhotoModel], cashbox_id: int):
        for_create_list = []
        for event in events_photo:
            query = (
                select(pictures.c.id)
                .where(and_(
                    pictures.c.owner == cashbox_id,
                    pictures.c.id == event.photo_id
                ))
            )
            result = await database.fetch_one(query)
            if result:
                for_create_list.append(
                    {
                        "booking_event_id": event.event_id,
                        "photo_id": event.photo_id
                    }
                )

        await self.__booking_events_repository.add_photos(
            events_photos=for_create_list
        )

    async def delete_by_ids(self, event_ids: List[int], cashbox_id: int):
        deleted_ids = await self.__booking_events_repository.get_by_ids(
            event_ids=event_ids,
            cashbox_id=cashbox_id
        )

        await self.__booking_events_repository.delete_photos_by_event_ids(
            event_ids=deleted_ids
        )

        await self.__booking_events_repository.delete_by_ids(
            event_ids=deleted_ids,
            cashbox_id=cashbox_id
        )

    async def delete_photos_by_ids(self, photo_ids: List[int], cashbox_id: int):
        query = (
            select(booking_events_photo.c.id)
            .join(pictures, booking_events_photo.c.photo_id == pictures.c.id)
            .where(and_(
                booking_events_photo.c.id.in_(photo_ids),
                pictures.c.owner == cashbox_id
            ))
        )
        photo_ids = await database.fetch_all(query)
        deleted_ids = [element.id for element in photo_ids]

        await self.__booking_events_repository.delete_photos_by_ids(
            photo_ids=deleted_ids
        )
