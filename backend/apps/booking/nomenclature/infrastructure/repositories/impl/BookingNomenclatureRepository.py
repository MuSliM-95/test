from sqlalchemy import select, and_

from apps.booking.nomenclature.infrastructure.repositories.core.IBookingNomenclatureRepository import \
    IBookingNomenclatureRepository
from database.db import booking_nomenclature, booking, database


class BookingNomenclatureRepository(IBookingNomenclatureRepository):

    async def get_by_id(self, cashbox: int, nomenclature_id: int):
        query = (
            select(
                booking_nomenclature.c.id
            )
            .join(booking, booking_nomenclature.c.booking_id == booking.c.id)
            .where(and_(
                booking_nomenclature.c.nomenclature_id == nomenclature_id,
                booking.c.cashbox == cashbox
            ))
        )
        booking_nomenclature_info = await database.fetch_one(query)
        return None if not booking_nomenclature_info else booking_nomenclature_info.id