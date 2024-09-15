from typing import List

from apps.booking.events.domain.models.CreateBookingEventModel import CreateBookingEventModel
from apps.booking.events.infrastructure.services.core.IBookingEventsService import IBookingEventsService
from functions.helpers import get_user_by_token


class CreateBookingEventsView:

    def __init__(
        self,
        booking_events_service: IBookingEventsService
    ):
        self.__booking_events_service = booking_events_service

    async def __call__(
        self,
        token: str, create_events: List[CreateBookingEventModel]
    ):
        user = await get_user_by_token(token)

        created_events = await self.__booking_events_service.add_more(
            events=create_events
        )
        return created_events
