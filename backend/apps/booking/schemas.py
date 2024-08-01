from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from database.db import Tariff, DocSalesStatus, BookingStatus


class NomenclatureBookingCreate(BaseModel):
    nomenclature_id: int
    tariff: Optional[Tariff] = None


class BookingCreate(BaseModel):
    contragent: int
    contragent_accept: int = None
    address: str = None
    date_booking: int
    start_booking: int = None
    end_booking: int = None
    status_doc_sales: DocSalesStatus
    status_booking: BookingStatus
    comment: str = None
    goods: Optional[List[NomenclatureBookingCreate]]


class BookingCreateList(BaseModel):
    __root__: Optional[List[BookingCreate]]


class Booking(BookingCreate):
    id: int
    is_deleted: bool
    created_at: int
    updated_at: int


class BookingList(BookingCreate):
    data: Optional[List[Booking]]







