import json

from api.docs_sales.routers import generate_and_save_order_links

from database.db import docs_sales_delivery_info, database


def format_contragent_text_notifications(action: str, segment_name: str, name: str, phone: str):
    if action == "new_contragent":
        header = "Новый пользователь добавлен в сегмент!"
    else:
        header = "Пользователь исключен из сегмента!"
    return f"{header}\nСегмент: {segment_name}.\nКлиент:\n{name}\nТелефон: {phone}"


async def create_replacements(order_id:int) -> dict:
    replacements = {}
    await link_replacements(replacements, order_id)
    await create_delivery_info_text(replacements, order_id)
    return replacements


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
        data["delivery_address"] = f"\n<b>Адрес доставки:</b>\n{delivery_info.address}\n\n"
    if delivery_info.note:
        data["delivery_note"] = f"\n<b>Комментарий к доставке:</b>\n{delivery_info.note}\n\n"
    if delivery_info.delivery_date:
        data["delivery_date"] = f"\n<b>Дата доставки:</b>\n{delivery_info.delivery_date}\n\n"
    if delivery_info.recipient:
        reciient_data = json.loads(delivery_info.recipient)
        data["delivery_recipient"] = (
            f"\n<b>Получатель:</b>\n{reciient_data.get('name')}\n\n"
            f"<b>Телефон:</b>\n{reciient_data.get('phone')}\n\n"
        )
    replacements.update(data)

