from fastapi import APIRouter, HTTPException, Depends, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from database.db import database, booking, booking_nomenclature, nomenclature, amo_leads_docs_sales_mapping, docs_sales, \
    amo_leads
from sqlalchemy import or_, and_, select
from functions.helpers import get_user_by_token
from apps.booking.schemas import ResponseCreate, BookingList, Booking, BookingCreateList, BookingEdit, \
    BookingEditList, NomenclatureBookingEdit, NomenclatureBookingCreate, BookingFiltersList
from ws_manager import manager


router = APIRouter(tags=["booking"])


async def create_filters_list(filters: BookingFiltersList):
    result = []

    if filters.dict().get("title"):
        result.append(booking.c.title.ilike(f'%{filters.dict().get("title").strip().lower()}%'))

    if filters.dict().get("contragent"):
        result.append(booking.c.contragent == filters.dict().get("contragent"))

    if filters.dict().get("start_booking") and filters.dict().get("end_booking"):
        result.append(and_(booking.c.start_booking >= filters.dict().get("start_booking"),
                           booking.c.end_booking <= filters.dict().get("end_booking")))
    elif filters.dict().get("start_booking") and not filters.dict().get("end_booking"):
        result.append(booking.c.start_booking >= filters.dict().get("start_booking"))
    elif filters.dict().get("end_booking") and not filters.dict().get("start_booking"):
        result.append(booking.c.end_booking <= filters.dict().get("end_booking"))

    if filters.dict().get("status_doc_sales"):
        result.append(booking.c.status_doc_sales == filters.dict().get("status_doc_sales"))

    if filters.dict().get("status_booking"):
        result.append(booking.c.status_booking == filters.dict().get("status_booking"))

    if filters.dict().get("status_booking") and filters.dict().get("status_doc_sales"):
        result.append(or_(booking.c.status_booking == filters.dict().get("status_booking"),
                          booking.c.status_doc_sales == filters.dict().get("status_doc_sales")))

    if filters.dict().get("status_booking") and not filters.dict().get("status_doc_sales"):
        result.append(booking.c.status_booking == filters.dict().get("status_booking"))

    if not filters.dict().get("status_booking") and filters.dict().get("status_doc_sales"):
        result.append(booking.c.status_doc_sales == filters.dict().get("status_doc_sales"))

    return result


@router.get("/booking/list", response_model = BookingList)
async def get_list_booking(token: str, filters: BookingFiltersList = Depends()):
    filter_result = await create_filters_list(filters)
    print(filter_result)
    user = await get_user_by_token(token)
    try:
        list_db = await database.fetch_all(select(booking).
                                           where(
            booking.c.cashbox == user.cashbox_id, *filter_result
        ))
        print(list(map(dict, list_db)))
        list_result = []
        for item in list(map(dict, list_db)):
            goods = await database.fetch_all(
                select(
                    booking_nomenclature.c.id,
                    booking_nomenclature.c.is_deleted,
                    booking_nomenclature.c.nomenclature_id,
                    booking_nomenclature.c.tariff,
                    nomenclature.c.name,
                    nomenclature.c.category
                )
                .where(and_(
                    booking_nomenclature.c.booking_id == item.get("id")
                ))
                .select_from(booking_nomenclature)
                .join(nomenclature, nomenclature.c.id == booking_nomenclature.c.nomenclature_id))
            list_result.append({**item, "goods": list(map(dict, goods))})

        print(list_result)
        return list_result
    except Exception as e:
        raise HTTPException(status_code = 432, detail = str(e))


@router.get("/booking/{idx}", response_model = Booking)
async def get_booking_by_idx(token: str, idx: int):
    user = await get_user_by_token(token)
    result = await database.fetch_one(booking.select().where(and_(
        booking.c.cashbox == user.cashbox_id,
        booking.c.id == idx,
        booking.c.is_deleted.is_not(True)
    )))
    if result:
        goods = await database.fetch_all(select(
                    booking_nomenclature.c.id,
                    booking_nomenclature.c.is_deleted,
                    booking_nomenclature.c.nomenclature_id,
                    booking_nomenclature.c.tariff,
                    nomenclature.c.name,
                    nomenclature.c.category
                )
                .where(and_(booking_nomenclature.c.booking_id == idx))
                .select_from(booking_nomenclature)
                .join(nomenclature, nomenclature.c.id == booking_nomenclature.c.nomenclature_id))
        dict_result = dict(result)
        dict_result['goods'] = list(map(dict, goods))
        print(dict_result)
        return dict_result
    else:
        raise HTTPException(status_code = 404, detail = "not found")


@database.transaction()
@router.post("/booking/create",
             status_code=201,
             response_model = ResponseCreate,
             responses={201: {"model": ResponseCreate}}
             )
async def create_booking(token: str, bookings: BookingCreateList):
    user = await get_user_by_token(token)
    create_ids = []
    try:
        for bookingItem in bookings.dict()["__root__"]:
                    goods = bookingItem["goods"]
                    del bookingItem["goods"]
                    create_booking_id = await database.execute(
                        booking.insert().values(
                            {**bookingItem, "cashbox": user.get("cashbox_id"), "is_deleted": False}))
                    create_ids.append(create_booking_id)
                    [
                        await database.execute(booking_nomenclature.insert().values(
                            {**good, "booking_id": create_booking_id, "is_deleted": False}))
                     for good in goods
                    ]
        response = {
                    "status": "success created",
                    "data": [
                        await get_booking_by_idx(
                            token=token,
                            idx = booking_id) for booking_id in create_ids
                    ]
                 }
        await manager.send_message(
            token,
            {
                "action": "create",
                "target": "booking",
                "result": jsonable_encoder(response),
            },
        )
        return JSONResponse(status_code = 201, content = jsonable_encoder(
                response))
    except Exception as e:
        raise HTTPException(status_code = 432, detail = str(e))


@database.transaction()
@router.patch("/booking/edit",
              description =
              '''
              <p><div style="color:red">Важно!</div> 
              <ul>
              <li>Если товары не редактируются то в запросе ключ goods не отправляется.</li>
              <li>Если отправить goods: [] - из бронирования будут удалены все товары товары.</li>
              <li>Добавление товаров без ID ведет к их добавлению в бронирование.</li>
              <li>Если отправить goods с изменным полем - произойдет изменение поля. Для этого в элементе goods 
              указывается id и с ним те поля которые хотите изменить в бронировании товара</li>
              </ul>
              </p>
              
              ''')
async def create_booking(token: str, bookings: BookingEditList):
    user = await get_user_by_token(token)

    try:
        bookings = bookings.dict(exclude_unset = True)
        for bookingItem in bookings["__root__"]:
            if not bookingItem.get('id'):
                raise Exception("не указан id бронирования")
            goods = None
            if bookingItem.get("goods"):
                goods = bookingItem.get("goods")
                del bookingItem["goods"]

            bookingItem_db = await database.fetch_one(booking.select().where(booking.c.id == bookingItem.get("id")))
            if not bookingItem_db:
                raise Exception("не найден id бронирования")
            bookingItem_db_model = BookingEdit(**bookingItem_db)
            update_data = bookingItem
            updated_item = bookingItem_db_model.copy(update = update_data)
            await database.execute(booking.update().where(
                booking.c.id == bookingItem_db.get("id")).values(updated_item.dict()))
            if goods is not None:
                goods_db = await database.fetch_all(
                    booking_nomenclature.select().where(booking_nomenclature.c.booking_id == bookingItem.get("id")))
                for good in goods:
                    if good.get('id'):
                        goodItem_db = await database.fetch_one(
                                booking_nomenclature.select().where(booking_nomenclature.c.id == good.get("id")))
                        goodItem_db_model = NomenclatureBookingEdit(**goodItem_db)
                        updated_goodItem = goodItem_db_model.copy(update = good)
                        await database.execute(
                                booking_nomenclature.update().where(
                                    booking_nomenclature.c.id == good.get("id")).values(updated_goodItem.dict()))
                    else:
                        await database.execute(booking_nomenclature.insert().values(
                            {**NomenclatureBookingCreate(**good).dict(), "booking_id":bookingItem.get("id"),
                             "is_deleted": False}))
                for good_db in goods_db:
                    if good_db.get('id') not in [good.get('id') for good in goods]:
                        print(good_db.get('id'), [good.get('id') for good in goods])
                        await database.execute(booking_nomenclature.delete().where(booking_nomenclature.c.id == good_db.get('id')))
        response = {"status": "success updated", "data": [await get_booking_by_idx( token=token,
                                                                                    idx = bookingItem.get('id'))
                                                          for bookingItem in bookings["__root__"]]}
        await manager.send_message(
            token,  {
                "action": "edit",
                "target": "booking",
                "result": jsonable_encoder(response),
            })
        return JSONResponse(status_code = 200,
                            content = jsonable_encoder(
                                response)
                            )

    except Exception as e:
        raise HTTPException(status_code = 432, detail = str(e))


@database.transaction()
@router.post("/booking/events/create",
             status_code=201,
             )
async def create_events(token: str):
    pass


@router.get("/booking/events", status_code=200)
async def get_list_events(token: str):
    pass


@router.get("/booking/events/{idx}", status_code=200)
async def get_event_by_idx(token: str, idx: int):
    pass
