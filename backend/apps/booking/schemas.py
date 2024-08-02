from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from database.db import Tariff, DocSalesStatus, BookingStatus


class NomenclatureBookingCreate(BaseModel):
    nomenclature_id: int
    tariff: Optional[Tariff] = None


class NomenclatureBookingEdit(NomenclatureBookingCreate):
    id: int
    is_deleted: bool = None


class NomenclatureBookingPatch(BaseModel):
    id: Optional[int] = None
    is_deleted: Optional[bool] = None
    nomenclature_id: Optional[int] = None
    tariff: Optional[Tariff] = None


class BookingView(BaseModel):
    contragent: Optional[int] = None
    contragent_accept: Optional[int] = None
    address: Optional[str] = None
    date_booking: Optional[int] = None
    start_booking: Optional[int] = None
    end_booking: Optional[int] = None
    status_doc_sales: Optional[DocSalesStatus] = None
    status_booking: Optional[BookingStatus] = None
    comment: Optional[str] = None
    is_deleted: Optional[bool] = None

    class Config:
        orm_mode = True


class BookingEdit(BookingView):
    id: Optional[int]


    class Config:
        orm_mode = True


class BookingEditGoods(BookingEdit):
    goods: Optional[List[NomenclatureBookingPatch]]


class BookingEditList(BaseModel):
    __root__: Optional[List[BookingEditGoods]]


class BookingCreate(BookingView):
    goods: Optional[List[NomenclatureBookingCreate]]


class BookingCreateList(BaseModel):
    __root__: Optional[List[BookingCreate]]


class Booking(BookingView):
    id: int
    is_deleted: Optional[bool] = None
    goods: Optional[List[NomenclatureBookingPatch]]


class BookingList(BaseModel):
    __root__: Optional[List[Booking]]


class ResponseCreate(BaseModel):
    status: str
    data: List[BookingCreate]







