import asyncio
import json
from datetime import datetime

from api.docs_sales.routers import generate_and_save_order_links

from database.db import (
    docs_sales_delivery_info, database, docs_sales, warehouses, users,
    users_cboxes_relation, docs_sales_goods, nomenclature, units, contragents,
)
from sqlalchemy import select, func


def format_contragent_text_notifications(action: str, segment_name: str, name: str, phone: str):
    if action == "new_contragent":
        header = "Новый пользователь добавлен в сегмент!"
    else:
        header = "Пользователь исключен из сегмента!"
    return f"{header}\nСегмент: {segment_name}.\nКлиент:\n{name}\nТелефон: {phone}"


async def create_replacements(order_id:int) -> dict:
    replacements = {}
    await asyncio.gather(
        add_warehouse_info_to_replacements(replacements, order_id),
        add_manager_info_to_replacements(replacements, order_id),
        add_docs_sales_goods_info_to_replacements(replacements, order_id),
        link_replacements(replacements, order_id),
        create_delivery_info_text(replacements, order_id),
        add_contragent_info_to_replacements(replacements, order_id)
    )
    return replacements


async def add_warehouse_info_to_replacements(replacements:dict, order_id:int):
    data = {}
    query = warehouses.select().join(docs_sales, docs_sales.c.warehouse == warehouses.c.id).where(docs_sales.c.id == order_id)
    row = await database.fetch_one(query)
    if row:
        data["warehouse"] = "\n"
        if row.name:
            data["warehouse"] += f"Склад: <b>{row.name}</b>\n"
        if row.address:
            data["warehouse"] += f"Адрес склада:\n {row.address}\n"
        if row.phone:
            data["warehouse"] += f"Телефон: {row.phone}\n"
    replacements.update(data)

async def add_manager_info_to_replacements(replacements:dict, order_id:int):
    data = {}
    order_query = docs_sales.select().where(docs_sales.c.id == order_id)
    order = await database.fetch_one(order_query)
    if not order:
        return
    query = users.select().join(users_cboxes_relation, users_cboxes_relation.c.user == users.c.id)
    user = await database.fetch_one(query)
    if user:
        data["manager"] = "<b>Менеджер:</b>\n"
        if user.first_name:
            data["manager"] += f"{user.first_name} "
        if user.last_name:
            data["manager"] += f"{user.last_name}"
        if user.phone_number:
            data["manager"] += f"\nТелефон: {user.phone_number}\n"

    replacements.update(data)

async def link_replacements(replacements, order_id):
    data = {}
    links = await generate_and_save_order_links(order_id)
    for k,v in links.items():
        data[k] = f"\n\n<a href='{v['url']}'>Открыть заказ</a>"

    replacements.update(data)


async def create_delivery_info_text(replacements: dict, docs_sales_id: int):
    query = docs_sales_delivery_info.select().where(docs_sales_delivery_info.c.docs_sales_id == docs_sales_id)
    delivery_info = await database.fetch_one(query)
    data = {}
    if delivery_info is None:
        return data
    if delivery_info.address:
        data["delivery_address"] = f"<b>Адрес доставки:</b>\n{delivery_info.address}\n"
    if delivery_info.note:
        data["delivery_note"] = f"<b>Комментарий к доставке:</b>\n{delivery_info.note}\n"
    if delivery_info.delivery_date:
        data["delivery_date"] = f"<b>Дата доставки:</b>\n{delivery_info.delivery_date.strftime('%d.%m.%Y %H:%M')}\n"
    if delivery_info.recipient:
        reciient_data = json.loads(delivery_info.recipient)
        data["delivery_recipient"] = (
            f"<b>Получатель:</b>\nИмя: {reciient_data.get('name')}\n"
            f"<b>Телефон:</b> {reciient_data.get('phone')}\n"
        )
    replacements.update(data)


async def add_docs_sales_goods_info_to_replacements(replacements:dict, docs_sales_id:int):
    data = {}
    subquery = docs_sales_goods.select().where(docs_sales_goods.c.docs_sales_id == docs_sales_id).subquery("goods")
    query = (
        select(nomenclature.c.name, subquery.c.price, subquery.c.quantity, units.c.convent_national_view)
        .outerjoin(nomenclature, subquery.c.nomenclature == nomenclature.c.id)
        .outerjoin(units, subquery.c.unit == units.c.id)
    )
    goods = await database.fetch_all(query)
    if goods:
        sum = 0
        data["goods"] = ""
        for good in goods:
            data["goods"] += (
                f"{good.name} - {good.quantity} "
                f"{good.convent_national_view if good.convent_national_view  else ''}"
                f" x {good.price} р = {good.quantity * good.price} р\n"
            )
            sum += good.quantity * good.price
        data["goods_count"] = f"\nКоличество товаров в заказе: {len(goods)}"
        data["order_sum"] = f"Сумма заказа: {sum}\n"
    replacements.update(data)


async def add_contragent_info_to_replacements(replacements: dict, docs_sales_id: int):
    data = {}

    query = (
        select(contragents.c.name, contragents.c.phone)
        .join(contragents, docs_sales.c.contragent == contragents.c.id)
        .where(docs_sales.c.id == docs_sales_id)
    )
    contragent = await database.fetch_one(query)
    if contragent:
        data["contragent"] = f"Заказчик: \nИмя: {contragent.name}\n"
        if contragent.phone:
            data["contragent"] += f"Телефон: {contragent.phone}\n"

    replacements.update(data)
