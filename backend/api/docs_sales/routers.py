import asyncio
import datetime
import hashlib
import os
import pprint
import secrets
from typing import Any, Dict, Optional, Union

from api.docs_warehouses.routers import update as update_warehouse_doc
from api.docs_warehouses.schemas import EditMass as WarehouseUpdate
from api.docs_warehouses.utils import create_warehouse_docs
from api.loyality_transactions.routers import raschet_bonuses
from apps.yookassa.functions.impl.GetOauthCredentialFunction import (
    GetOauthCredentialFunction,
)
from apps.yookassa.models.PaymentModel import (
    AmountModel,
    ConfirmationRedirect,
    CustomerModel,
    ItemModel,
    PaymentCreateModel,
    ReceiptModel,
    
)
from apps.yookassa.repositories.impl.YookassaTableNomenclature import (
    YookassaTableNomenclature,
)
from apps.yookassa.repositories.impl.YookasssaAmoTableCrmRepository import (
    YookasssaAmoTableCrmRepository,
)
from apps.yookassa.repositories.impl.YookassaCrmPaymentsRepository import (
    YookassaCrmPaymentsRepository,
)
from apps.yookassa.repositories.impl.YookassaOauthRepository import (
    YookassaOauthRepository,
)
from apps.yookassa.repositories.impl.YookassaPaymentsRepository import (
    YookassaPaymentsRepository,
)
from apps.yookassa.repositories.impl.YookassaRequestRepository import (
    YookassaRequestRepository,
)
from apps.yookassa.services.impl.OauthService import OauthService
from apps.yookassa.services.impl.YookassaApiService import YookassaApiService

from database.db import (
    NomenclatureCashbackType,
    OrderStatus,
    articles,
    contracts,
    contragents,
    database,
    docs_sales,
    docs_sales_delivery_info,
    docs_sales_goods,
    docs_sales_links,
    docs_sales_settings,
    docs_sales_tags,
    docs_warehouse,
    entity_to_entity,
    loyality_cards,
    loyality_transactions,
    nomenclature,
    organizations,
    payments,
    pboxes,
    price_types,
    users,
    users_cboxes_relation,
    warehouse_balances,
    warehouses,
    segment_objects,
    SegmentObjectType,
    Role,
)
from fastapi import APIRouter, Depends, HTTPException
from functions.helpers import (
    add_delivery_info_to_doc,
    add_docs_sales_settings,
    add_nomenclature_name_to_goods,
    check_contragent_exists,
    check_entity_exists,
    check_period_blocked,
    check_unit_exists,
    datetime_to_timestamp,
    get_user_by_token,
    raschet_oplat,
)
from functions.users import raschet
from producer import queue_notification
from sqlalchemy import and_, desc, func, select, exists, or_, String, cast
from ws_manager import manager

from . import schemas
from .notify_service import format_notification_text, send_order_notification

from api.employee_shifts.service import (
    check_user_on_shift,
    get_available_pickers_on_shift,
    get_available_couriers_on_shift
)

router = APIRouter(tags=["docs_sales"])

contragents_cache = set()
organizations_cache = set()
contracts_cache = set()
warehouses_cache = set()
users_cache = set()
price_types_cache = set()
units_cache = set()

# Секретный ключ для генерации MD5-хешей (в реальном приложении лучше хранить в переменных окружения)
SECRET_KEY = os.environ.get(
    "MD5_SECRET_KEY", "default_secret_key_for_notification_hashes"
)


def generate_notification_hash(order_id: int, role: str) -> str:
    """Генерация MD5-хеша для уведомлений на основе ID заказа и роли"""
    data = f"{order_id}:{role}:{SECRET_KEY}"
    return hashlib.md5(data.encode()).hexdigest()


async def generate_and_save_order_links(order_id: int) -> dict:
    """
    Генерирует и сохраняет ссылки для заказа для разных ролей

    Args:
        order_id: ID заказа

    Returns:
        dict: Словарь с сгенерированными ссылками
    """
    # Проверка существования заказа
    query = docs_sales.select().where(docs_sales.c.id == order_id)
    order = await database.fetch_one(query)

    if not order:
        return None

    # Получаем базовый URL
    base_url = os.environ.get("APP_URL")
    if not base_url:
        raise ValueError("APP_URL не задан в переменных окружения")

    # Генерация хешей и URL для каждой роли
    roles = ["general", "picker", "courier"]
    links = {}

    for role in roles:
        # Проверяем, существует ли уже ссылка для этой роли и заказа
        query = docs_sales_links.select().where(
            docs_sales_links.c.docs_sales_id == order_id,
            docs_sales_links.c.role == role,
        )
        existing_link = await database.fetch_one(query)

        if existing_link:
            # Если ссылка уже существует, используем её
            link_dict = dict(existing_link)
            # Преобразуем role из enum в строку, получая только значение
            link_dict["role"] = (
                link_dict["role"].value
                if hasattr(link_dict["role"], "value")
                else link_dict["role"].name
            )
            links[f"{role}_link"] = link_dict
        else:
            # Генерация нового хеша
            hash_value = generate_notification_hash(order_id, role)

            # Формирование URL
            if role == "general":
                url = f"{base_url}/orders/{order_id}?hash={hash_value}"
            else:
                url = f"{base_url}/orders/{order_id}/{role}?hash={hash_value}"

            # Сохраняем в базу данных
            query = docs_sales_links.insert().values(
                docs_sales_id=order_id, role=role, hash=hash_value, url=url
            )
            link_id = await database.execute(query)

            # Получаем созданную запись
            query = docs_sales_links.select().where(docs_sales_links.c.id == link_id)
            created_link = await database.fetch_one(query)

            link_dict = dict(created_link)
            # Преобразуем role из enum в строку, получая только значение
            link_dict["role"] = (
                link_dict["role"].value
                if hasattr(link_dict["role"], "value")
                else link_dict["role"].name
            )
            links[f"{role}_link"] = link_dict

    return links


async def exists_settings_docs_sales(docs_sales_id: int) -> bool:
    query = docs_sales.select().where(
        docs_sales.c.id == docs_sales_id, docs_sales.c.settings.is_not(None)
    )
    exists = await database.fetch_one(query)
    return bool(exists)


async def add_settings_docs_sales(settings: Optional[dict]) -> Optional[int]:
    if settings:
        query = docs_sales_settings.insert().values(settings)
        docs_sales_settings_id = await database.execute(query)
        return docs_sales_settings_id


async def update_settings_docs_sales(
    docs_sales_id: int, settings: Optional[dict]
) -> None:
    if settings:
        docs_sales_ids = (
            select(docs_sales.c.settings)
            .where(docs_sales.c.id == docs_sales_id)
            .subquery("docs_sales_ids")
        )
        query = (
            docs_sales_settings.update()
            .where(docs_sales_settings.c.id.in_(docs_sales_ids))
            .values(settings)
        )
        await database.execute(query)


@router.get("/docs_sales/{idx}/", response_model=schemas.View)
async def get_by_id(token: str, idx: int):
    """Получение документа по ID"""
    await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id == idx, docs_sales.c.is_deleted.is_not(True)
    )
    instance_db = await database.fetch_one(query)

    if not instance_db:
        raise HTTPException(status_code=404, detail="Не найдено.")

    # Приводим к удобному виду и делаем доп. вычисления
    instance_db = datetime_to_timestamp(instance_db)
    instance_db = await raschet_oplat(instance_db)
    instance_db = await add_docs_sales_settings(instance_db)

    # Загружаем товары
    query = docs_sales_goods.select().where(docs_sales_goods.c.docs_sales_id == idx)
    goods_db = await database.fetch_all(query)
    goods_db = [*map(datetime_to_timestamp, goods_db)]
    goods_db = [*map(add_nomenclature_name_to_goods, goods_db)]
    goods_db = [await instance for instance in goods_db]
    instance_db["goods"] = goods_db

    # Добавляем информацию о доставке
    instance_db = await add_delivery_info_to_doc(instance_db)

    # Сегменты контрагента
    contragent_id = instance_db.get("contragent")
    query = (
        select(segment_objects.c.segment_id)
        .where(
            segment_objects.c.object_id == contragent_id,
            segment_objects.c.object_type == SegmentObjectType.contragents,
        )
        .distinct()
    )
    res = await database.fetch_all(query)
    instance_db["contragent_segments"] = [r["segment_id"] for r in res]

    # ---------- Разворачиваем assigned_picker / assigned_courier ----------
    # Собираем все id, которые нужно загрузить
    picker_id = instance_db.get("assigned_picker")
    courier_id = instance_db.get("assigned_courier")

    ids_to_load = set()
    def collect_id(v):
        if v is None:
            return
        # если это объект вида {"id": ...}
        if isinstance(v, dict) and v.get("id"):
            try:
                ids_to_load.add(int(v.get("id")))
            except Exception:
                pass
        # если это число/строка — пробуем привести к int
        elif isinstance(v, (int, str)):
            try:
                ids_to_load.add(int(v))
            except Exception:
                pass

    collect_id(picker_id)
    collect_id(courier_id)

    users_map = {}
    if ids_to_load:
        user_rows = await database.fetch_all(users.select().where(users.c.id.in_(list(ids_to_load))))
        for u in user_rows:
            # u может быть RowProxy; приводим к dict-подобному доступу
            users_map[int(u["id"])] = {
                "id": int(u["id"]),
                "first_name": u.get("first_name"),
                "last_name": u.get("last_name"),
            }

    # Замена полей (если пользователь найден — заменяем на объект)
    def expand_field(orig):
        if orig is None:
            return None
        if isinstance(orig, dict) and orig.get("first_name") is not None or (isinstance(orig, dict) and orig.get("last_name") is not None):
            # уже развёрнутый объект — возвращаем как есть
            return orig
        # если вложенный id
        if isinstance(orig, dict) and orig.get("id"):
            try:
                aid = int(orig.get("id"))
                return users_map.get(aid, orig)
            except Exception:
                return orig
        # если просто id
        if isinstance(orig, (int, str)):
            try:
                aid = int(orig)
                return users_map.get(aid, orig)
            except Exception:
                return orig
        # иначе возвращаем как есть
        return orig

    instance_db["assigned_picker"] = expand_field(picker_id)
    instance_db["assigned_courier"] = expand_field(courier_id)

    return instance_db


@router.get("/docs_sales/", response_model=schemas.CountRes)
async def get_list(
    token: str,
    limit: int = 100,
    offset: int = 0,
    show_goods: bool = False,
    filters: schemas.FilterSchema = Depends(),
    kanban: bool = False,
):
    """Получение списка документов"""
    user = await get_user_by_token(token)

    query = (
        select(*docs_sales.columns, contragents.c.name.label("contragent_name"))
        .select_from(docs_sales)
        .outerjoin(contragents, docs_sales.c.contragent == contragents.c.id)
        .where(
            docs_sales.c.is_deleted.is_not(True),
            docs_sales.c.cashbox == user.cashbox_id,
        )
        .limit(limit)
        .offset(offset)
        .order_by(desc(docs_sales.c.id))
    )
    count_query = (
        select(func.count())
        .select_from(docs_sales)
        .where(
            docs_sales.c.is_deleted.is_not(True),
            docs_sales.c.cashbox == user.cashbox_id,
        )
    )

    filters_dict = filters.dict(exclude_none=True)
    filter_list = []

    # Фильтрация по конкретному сборщику
    if "picker_id" in filters_dict:
        pid = filters_dict["picker_id"]
        # предполагается, что docs_sales_links.c.user_id содержит ID сотрудника
        picker_exists = exists().where(
            and_(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.picker,
            )
        )
        filter_list.append(picker_exists)

    # Фильтрация по конкретному курьеру/логисту
    if "courier_id" in filters_dict:
        cid = filters_dict["courier_id"]
        courier_exists = exists().where(
            and_(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.courier,
            )
        )
        filter_list.append(courier_exists)

    if "delivery_date_from" in filters_dict or "delivery_date_to" in filters_dict:
        # Присоединяем таблицу с информацией о доставке
        query = query.outerjoin(
            docs_sales_delivery_info,
            docs_sales_delivery_info.c.docs_sales_id == docs_sales.c.id
        )

        # Фильтр по началу периода доставки
        if "delivery_date_from" in filters_dict:
            filter_list.append(
                docs_sales_delivery_info.c.delivery_date >= datetime.datetime.fromtimestamp(filters_dict["delivery_date_from"])
            )

        # Фильтр по концу периода доставки
        if "delivery_date_to" in filters_dict:
            filter_list.append(
                docs_sales_delivery_info.c.delivery_date <= datetime.datetime.fromtimestamp(filters_dict["delivery_date_to"])
            )

    # Обработка has_delivery
    if "has_delivery" in filters_dict:
        address_valid = and_(
            docs_sales_delivery_info.c.address.isnot(None),
            func.trim(cast(docs_sales_delivery_info.c.address, String)) != ''
        )

        note_valid = and_(
            docs_sales_delivery_info.c.note.isnot(None),
            func.trim(cast(docs_sales_delivery_info.c.note, String)) != ''
        )

        # delivery_date: если у вас timestamp -> просто isnot(None); 
        # если у вас unix-int и 0 означает "пусто" — добавьте != 0
        delivery_date_valid = docs_sales_delivery_info.c.delivery_date.isnot(None)

        # recipient: приводим к тексту и отсеиваем '{}' / 'null' / пустую строку
        recipient_text = func.trim(cast(docs_sales_delivery_info.c.recipient, String))
        recipient_valid = and_(
            docs_sales_delivery_info.c.recipient.isnot(None),
            recipient_text != '',
            recipient_text != '{}',
            recipient_text != 'null'
        )

        delivery_any_valid = or_(
            address_valid,
            note_valid,
            delivery_date_valid,
            recipient_valid,
        )

        # --- сам exists() ---
        delivery_exists = exists().where(
            and_(
                docs_sales_delivery_info.c.docs_sales_id == docs_sales.c.id,
                delivery_any_valid
            )
        )

        # Вставляем в фильтр
        if filters_dict.get("has_delivery") is True:
            filter_list.append(delivery_exists)
        elif filters_dict.get("has_delivery") is False:
            filter_list.append(~delivery_exists)

    # Обработка has_picker
    if "has_picker" in filters_dict:
        if filters_dict["has_picker"]:
            picker_exists = exists().where(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.picker
            )
            filter_list.append(picker_exists)
        else:
            picker_not_exists = ~exists().where(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.picker
            )
            filter_list.append(picker_not_exists)

    # Обработка has_courier
    if "has_courier" in filters_dict:
        if filters_dict["has_courier"]:
            courier_exists = exists().where(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.courier
            )
            filter_list.append(courier_exists)
        else:
            courier_not_exists = ~exists().where(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.courier
            )
            filter_list.append(courier_not_exists)

    if "order_status" in filters_dict:
        statuses = filters_dict["order_status"].split(",")
        if statuses:
            filter_list.append(docs_sales.c.order_status.in_(statuses))

    if "priority" in filters_dict:
        value = filters_dict["priority"]
        if isinstance(value, dict):
            # Поддержка {">": 5, "<": 10}
            for op, val in value.items():
                if op == "gt": filter_list.append(docs_sales.c.priority > val)
                if op == "lt": filter_list.append(docs_sales.c.priority < val)
        else:
            filter_list.append(docs_sales.c.priority == value)
    for k, v in filters_dict.items():
        if k in ["has_delivery", "has_picker", "has_courier", "priority", "order_status", "delivery_date_from", "delivery_date_to", "picker_id", "courier_id"]:
            continue
        if k.split("_")[-1] == "from":
            dated_from_param_value = func.to_timestamp(v)
            creation_date = func.to_timestamp(docs_sales.c.dated)
            dated_to_param_value = func.to_timestamp(
                filters_dict.get(k.replace("from", "to"))
            )
            filter_list.append(
                and_(
                    dated_from_param_value <= creation_date,
                    creation_date <= dated_to_param_value,
                )
            )

        elif k.split("_")[-1] == "to":
            continue

        elif type(v) is bool:
            filter_list.append(and_(eval(f"docs_sales.c.{k}.is_({v})")))

        elif type(v) is str:
            filter_list.append(
                and_(
                    *list(
                        map(
                            lambda x: eval(
                                f"docs_sales.c.{k}.ilike(f'%{x.strip().lower()}%')"
                            ),
                            v.strip().split(","),
                        )
                    )
                )
            )
        else:
            filter_list.append(and_(eval(f"docs_sales.c.{k} == {v}")))

    query = query.filter(and_(*filter_list))

    count_query = count_query.filter(and_(*filter_list))

    items_db = await database.fetch_all(query)
    count = await database.fetch_val(count_query)

    items_db = [*map(datetime_to_timestamp, items_db)]

    doc_ids = [item["id"] for item in items_db]

    doc_has_loyality = {}

    if doc_ids:
        loyality_query = entity_to_entity.select().where(
            and_(
                entity_to_entity.c.from_entity == 7,  # docs_sales
                entity_to_entity.c.from_id.in_(doc_ids),  # ID документов продаж
                entity_to_entity.c.to_entity == 6,  # loyality_transactions
                entity_to_entity.c.type
                == "docs_sales_loyality_transactions",  # тип связи
                entity_to_entity.c.status.is_(True),
                entity_to_entity.c.delinked.is_not(True),
            )
        )

        loyality_data = await database.fetch_all(loyality_query)

        loyality_data = [dict(record) for record in loyality_data]
        print("loyality_data", loyality_data)

        for record in loyality_data:
            doc_has_loyality[record["from_id"]] = record["to_id"]

    goods_query = docs_sales_goods.select().where(
        docs_sales_goods.c.docs_sales_id.in_(doc_ids)
    )
    goods_data = await database.fetch_all(goods_query)
    goods_map = {}

    for good in goods_data:
        doc_id = good["docs_sales_id"]
        if doc_id not in goods_map:
            goods_map[doc_id] = []
        goods_map[doc_id].append(good)

    # --- предварительная обработка каждого item (delivery info, counts, скидки) ---
    for item in items_db:
        item = await add_delivery_info_to_doc(item)
        goods = goods_map.get(item["id"], [])
        item["nomenclature_count"] = len(goods)
        item["doc_discount"] = round(
            sum(good.get("sum_discounted", 0) or 0 for good in goods), 2
        )

        contragent_id = item.get("contragent")
        item["has_contragent"] = bool(contragent_id)

        if contragent_id:
            query = (
                select(segment_objects.c.segment_id)
                .where(segment_objects.c.object_id == contragent_id,
                       segment_objects.c.object_type == SegmentObjectType.contragents)
                .distinct()
            )
            res = await database.fetch_all(query)
            item["contragent_segments"] = [r["segment_id"] for r in res]

        has_loyality = item["id"] in doc_has_loyality
        item["has_loyality_card"] = has_loyality

        if has_loyality:
            item["color_status"] = "green"
        elif item["has_contragent"]:
            item["color_status"] = "blue"
        else:
            item["color_status"] = "default"

    settings_ids = [item["settings"] for item in items_db]
    settings_query = docs_sales_settings.select().where(
        docs_sales_settings.c.id.in_(settings_ids)
    )
    settings_data = await database.fetch_all(settings_query)
    settings_map = {setting["id"]: setting for setting in settings_data}

    for item in items_db:
        item["settings"] = settings_map.get(item["settings"])

    items_db = await asyncio.gather(*[raschet_oplat(item) for item in items_db])

    items_db = [*map(raschet_oplat, items_db)]
    items_db = [await instance for instance in items_db]

    if show_goods:
        for item in items_db:
            item_goods = goods_map.get(item["id"], [])
            item["goods"] = [
                add_nomenclature_name_to_goods(good) for good in item_goods
            ]

    if show_goods:
        for item in items_db:
            query = docs_sales_goods.select().where(
                docs_sales_goods.c.docs_sales_id == item["id"]
            )
            goods_db = await database.fetch_all(query)
            goods_db = [*map(datetime_to_timestamp, goods_db)]

            goods_db = await asyncio.gather(
                *[add_nomenclature_name_to_goods(good) for good in goods_db]
            )

            item["goods"] = goods_db

    pprint.pprint(items_db)
    return {"result": items_db, "count": count}

@router.get("/docs_sales/created/{date}", response_model=schemas.CountRes)
async def get_list_by_created_date(
    token: str,
    date: str,
    show_goods: bool = False,
    filters: schemas.FilterSchema = Depends(),
    kanban: bool = False,
):
    """Получение списка документов"""
    user = await get_user_by_token(token)

    try:
        date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")
        start_of_day = int(date_obj.replace(hour=0, minute=0, second=0).timestamp())
        end_of_day = int(date_obj.replace(hour=23, minute=59, second=59).timestamp())
    except ValueError:
        raise HTTPException(status_code=400, detail="Неверный формат даты. Используйте YYYY-MM-DD")

    query = (
        select(*docs_sales.columns, contragents.c.name.label("contragent_name"))
        .select_from(docs_sales)
        .outerjoin(contragents, docs_sales.c.contragent == contragents.c.id)
        .where(
            docs_sales.c.is_deleted.is_not(True),
            docs_sales.c.cashbox == user.cashbox_id,
            docs_sales.c.dated >= start_of_day,
            docs_sales.c.dated <= end_of_day,
        )
        .order_by(desc(docs_sales.c.id))
    )
    count_query = (
        select(func.count())
        .select_from(docs_sales)
        .where(
            docs_sales.c.is_deleted.is_not(True),
            docs_sales.c.cashbox == user.cashbox_id,
            docs_sales.c.dated >= start_of_day,
            docs_sales.c.dated <= end_of_day
        )
    )

    filters_dict = filters.dict(exclude_none=True)
    filter_list = []

    # Фильтрация по конкретному сборщику
    if "picker_id" in filters_dict:
        pid = filters_dict["picker_id"]
        # предполагается, что docs_sales_links.c.user_id содержит ID сотрудника
        picker_exists = exists().where(
            and_(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.picker,
            )
        )
        filter_list.append(picker_exists)

    # Фильтрация по конкретному курьеру/логисту
    if "courier_id" in filters_dict:
        cid = filters_dict["courier_id"]
        courier_exists = exists().where(
            and_(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.courier,
            )
        )
        filter_list.append(courier_exists)

    if "delivery_date_from" in filters_dict or "delivery_date_to" in filters_dict:
        # Присоединяем таблицу с информацией о доставке
        query = query.outerjoin(
            docs_sales_delivery_info,
            docs_sales_delivery_info.c.docs_sales_id == docs_sales.c.id
        )

        # Фильтр по началу периода доставки
        if "delivery_date_from" in filters_dict:
            filter_list.append(
                docs_sales_delivery_info.c.delivery_date >= datetime.datetime.fromtimestamp(filters_dict["delivery_date_from"])
            )

        # Фильтр по концу периода доставки
        if "delivery_date_to" in filters_dict:
            filter_list.append(
                docs_sales_delivery_info.c.delivery_date <= datetime.datetime.fromtimestamp(filters_dict["delivery_date_to"])
            )

    # Обработка has_delivery
    if "has_delivery" in filters_dict:
        address_valid = and_(
            docs_sales_delivery_info.c.address.isnot(None),
            func.trim(cast(docs_sales_delivery_info.c.address, String)) != ''
        )

        note_valid = and_(
            docs_sales_delivery_info.c.note.isnot(None),
            func.trim(cast(docs_sales_delivery_info.c.note, String)) != ''
        )

        # delivery_date: если у вас timestamp -> просто isnot(None); 
        # если у вас unix-int и 0 означает "пусто" — добавьте != 0
        delivery_date_valid = docs_sales_delivery_info.c.delivery_date.isnot(None)

        # recipient: приводим к тексту и отсеиваем '{}' / 'null' / пустую строку
        recipient_text = func.trim(cast(docs_sales_delivery_info.c.recipient, String))
        recipient_valid = and_(
            docs_sales_delivery_info.c.recipient.isnot(None),
            recipient_text != '',
            recipient_text != '{}',
            recipient_text != 'null'
        )

        delivery_any_valid = or_(
            address_valid,
            note_valid,
            delivery_date_valid,
            recipient_valid,
        )

        # --- сам exists() ---
        delivery_exists = exists().where(
            and_(
                docs_sales_delivery_info.c.docs_sales_id == docs_sales.c.id,
                delivery_any_valid
            )
        )

        # Вставляем в фильтр
        if filters_dict.get("has_delivery") is True:
            filter_list.append(delivery_exists)
        elif filters_dict.get("has_delivery") is False:
            filter_list.append(~delivery_exists)

    # Обработка has_picker
    if "has_picker" in filters_dict:
        if filters_dict["has_picker"]:
            picker_exists = exists().where(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.picker
            )
            filter_list.append(picker_exists)
        else:
            picker_not_exists = ~exists().where(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.picker
            )
            filter_list.append(picker_not_exists)

    # Обработка has_courier
    if "has_courier" in filters_dict:
        if filters_dict["has_courier"]:
            courier_exists = exists().where(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.courier
            )
            filter_list.append(courier_exists)
        else:
            courier_not_exists = ~exists().where(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.courier
            )
            filter_list.append(courier_not_exists)

    if "order_status" in filters_dict:
        statuses = filters_dict["order_status"].split(",")
        if statuses:
            filter_list.append(docs_sales.c.order_status.in_(statuses))

    if "priority" in filters_dict:
        value = filters_dict["priority"]
        if isinstance(value, dict):
            # Поддержка {">": 5, "<": 10}
            for op, val in value.items():
                if op == "gt": filter_list.append(docs_sales.c.priority > val)
                if op == "lt": filter_list.append(docs_sales.c.priority < val)
        else:
            filter_list.append(docs_sales.c.priority == value)
    for k, v in filters_dict.items():
        if k in ["has_delivery", "has_picker", "has_courier", "priority", "order_status", "delivery_date_from", "delivery_date_to", "picker_id", "courier_id"]:
            continue
        if k.split("_")[-1] == "from":
            dated_from_param_value = func.to_timestamp(v)
            creation_date = func.to_timestamp(docs_sales.c.dated)
            dated_to_param_value = func.to_timestamp(
                filters_dict.get(k.replace("from", "to"))
            )
            filter_list.append(
                and_(
                    dated_from_param_value <= creation_date,
                    creation_date <= dated_to_param_value,
                )
            )

        elif k.split("_")[-1] == "to":
            continue

        elif type(v) is bool:
            filter_list.append(and_(eval(f"docs_sales.c.{k}.is_({v})")))

        elif type(v) is str:
            filter_list.append(
                and_(
                    *list(
                        map(
                            lambda x: eval(
                                f"docs_sales.c.{k}.ilike(f'%{x.strip().lower()}%')"
                            ),
                            v.strip().split(","),
                        )
                    )
                )
            )
        else:
            filter_list.append(and_(eval(f"docs_sales.c.{k} == {v}")))

    query = query.filter(and_(*filter_list))

    count_query = count_query.filter(and_(*filter_list))

    items_db = await database.fetch_all(query)
    count = await database.fetch_val(count_query)

    items_db = [*map(datetime_to_timestamp, items_db)]

    doc_ids = [item["id"] for item in items_db]

    doc_has_loyality = {}

    if doc_ids:
        loyality_query = entity_to_entity.select().where(
            and_(
                entity_to_entity.c.from_entity == 7,  # docs_sales
                entity_to_entity.c.from_id.in_(doc_ids),  # ID документов продаж
                entity_to_entity.c.to_entity == 6,  # loyality_transactions
                entity_to_entity.c.type
                == "docs_sales_loyality_transactions",  # тип связи
                entity_to_entity.c.status.is_(True),
                entity_to_entity.c.delinked.is_not(True),
            )
        )

        loyality_data = await database.fetch_all(loyality_query)

        loyality_data = [dict(record) for record in loyality_data]
        print("loyality_data", loyality_data)

        for record in loyality_data:
            doc_has_loyality[record["from_id"]] = record["to_id"]

    goods_query = docs_sales_goods.select().where(
        docs_sales_goods.c.docs_sales_id.in_(doc_ids)
    )
    goods_data = await database.fetch_all(goods_query)
    goods_map = {}

    for good in goods_data:
        doc_id = good["docs_sales_id"]
        if doc_id not in goods_map:
            goods_map[doc_id] = []
        goods_map[doc_id].append(good)

    # --- предварительная обработка каждого item (delivery info, counts, скидки) ---
    for item in items_db:
        item = await add_delivery_info_to_doc(item)
        goods = goods_map.get(item["id"], [])
        item["nomenclature_count"] = len(goods)
        item["doc_discount"] = round(
            sum(good.get("sum_discounted", 0) or 0 for good in goods), 2
        )

        contragent_id = item.get("contragent")
        item["has_contragent"] = bool(contragent_id)

        if contragent_id:
            query = (
                select(segment_objects.c.segment_id)
                .where(segment_objects.c.object_id == contragent_id,
                       segment_objects.c.object_type == SegmentObjectType.contragents)
                .distinct()
            )
            res = await database.fetch_all(query)
            item["contragent_segments"] = [r["segment_id"] for r in res]

        has_loyality = item["id"] in doc_has_loyality
        item["has_loyality_card"] = has_loyality

        if has_loyality:
            item["color_status"] = "green"
        elif item["has_contragent"]:
            item["color_status"] = "blue"
        else:
            item["color_status"] = "default"

    settings_ids = [item["settings"] for item in items_db]
    settings_query = docs_sales_settings.select().where(
        docs_sales_settings.c.id.in_(settings_ids)
    )
    settings_data = await database.fetch_all(settings_query)
    settings_map = {setting["id"]: setting for setting in settings_data}

    for item in items_db:
        item["settings"] = settings_map.get(item["settings"])

    items_db = await asyncio.gather(*[raschet_oplat(item) for item in items_db])

    items_db = [*map(raschet_oplat, items_db)]
    items_db = [await instance for instance in items_db]

    # --- Пакетная загрузка пользователей для assigned_picker/assigned_courier ---
    # Собираем все уникальные id, которые нужно развернуть
    picker_ids = set()
    courier_ids = set()
    for item in items_db:
        ap = item.get("assigned_picker")
        ac = item.get("assigned_courier")
        # если поле — объект с вложенным id, берем вложенный id
        if isinstance(ap, dict) and ap.get("id"):
            picker_ids.add(ap.get("id"))
        elif isinstance(ap, (int, str)) and str(ap).strip() != "":
            try:
                picker_ids.add(int(ap))
            except Exception:
                # не число — игнорируем
                pass

        if isinstance(ac, dict) and ac.get("id"):
            courier_ids.add(ac.get("id"))
        elif isinstance(ac, (int, str)) and str(ac).strip() != "":
            try:
                courier_ids.add(int(ac))
            except Exception:
                pass

    user_ids = list(picker_ids.union(courier_ids))

    users_map = {}
    if user_ids:
        user_rows = await database.fetch_all(users.select().where(users.c.id.in_(user_ids)))
        # преобразуем в dict для быстрого доступа
        for u in user_rows:
            users_map[u["id"]] = u

    # Заменяем id на объект { id, first_name, last_name } там, где возможно
    for item in items_db:
        ap = item.get("assigned_picker")
        if ap is not None:
            # если уже объект с полями — используем его (но если это число — разворачиваем по users_map)
            if isinstance(ap, dict):
                # может быть уже развёрнутый объект — оставляем как есть
                if not (ap.get("first_name") or ap.get("last_name")) and ap.get("id") in users_map:
                    u = users_map.get(ap.get("id"))
                    if u:
                        item["assigned_picker"] = {
                            "id": u["id"],
                            "first_name": u.get("first_name"),
                            "last_name": u.get("last_name"),
                        }
            else:
                # ap — id (число/строка) — пытаемся развёрнуть
                try:
                    aid = int(ap)
                    u = users_map.get(aid)
                    if u:
                        item["assigned_picker"] = {
                            "id": u["id"],
                            "first_name": u.get("first_name"),
                            "last_name": u.get("last_name"),
                        }
                    else:
                        # если пользователя нет в мапе — оставляем как ID
                        item["assigned_picker"] = ap
                except Exception:
                    item["assigned_picker"] = ap

        ac = item.get("assigned_courier")
        if ac is not None:
            if isinstance(ac, dict):
                if not (ac.get("first_name") or ac.get("last_name")) and ac.get("id") in users_map:
                    u = users_map.get(ac.get("id"))
                    if u:
                        item["assigned_courier"] = {
                            "id": u["id"],
                            "first_name": u.get("first_name"),
                            "last_name": u.get("last_name"),
                        }
            else:
                try:
                    aid = int(ac)
                    u = users_map.get(aid)
                    if u:
                        item["assigned_courier"] = {
                            "id": u["id"],
                            "first_name": u.get("first_name"),
                            "last_name": u.get("last_name"),
                        }
                    else:
                        item["assigned_courier"] = ac
                except Exception:
                    item["assigned_courier"] = ac

    if show_goods:
        for item in items_db:
            item_goods = goods_map.get(item["id"], [])
            item["goods"] = [
                add_nomenclature_name_to_goods(good) for good in item_goods
            ]

    if show_goods:
        for item in items_db:
            query = docs_sales_goods.select().where(
                docs_sales_goods.c.docs_sales_id == item["id"]
            )
            goods_db = await database.fetch_all(query)
            goods_db = [*map(datetime_to_timestamp, goods_db)]

            goods_db = await asyncio.gather(
                *[add_nomenclature_name_to_goods(good) for good in goods_db]
            )

            item["goods"] = goods_db

    return {"result": items_db, "count": count}


@router.get("/docs_sales/delivery/{date}", response_model=schemas.CountRes)
async def get_list_by_delivery_date(
    token: str,
    date: str,
    show_goods: bool = False,
    filters: schemas.FilterSchema = Depends(),
    kanban: bool = False,
):
    """Получение списка документов"""
    user = await get_user_by_token(token)

    try:
        date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")
        start_of_day = date_obj.replace(hour=0, minute=0, second=0)
        end_of_day = date_obj.replace(hour=23, minute=59, second=59)
    except ValueError:
        raise HTTPException(status_code=400, detail="Неверный формат даты. Используйте YYYY-MM-DD")

    query = (
        select(*docs_sales.columns, contragents.c.name.label("contragent_name"))
        .select_from(docs_sales)
        .outerjoin(contragents, docs_sales.c.contragent == contragents.c.id)
        .outerjoin(
            docs_sales_delivery_info,
            docs_sales_delivery_info.c.docs_sales_id == docs_sales.c.id
        )
        .where(
            docs_sales.c.is_deleted.is_not(True),
            docs_sales.c.cashbox == user.cashbox_id,
            docs_sales_delivery_info.c.delivery_date >= start_of_day,
            docs_sales_delivery_info.c.delivery_date <= end_of_day
        )
        .order_by(desc(docs_sales.c.id))
    )
    count_query = (
        select(func.count())
        .select_from(docs_sales)
        .outerjoin(
            docs_sales_delivery_info,
            docs_sales_delivery_info.c.docs_sales_id == docs_sales.c.id
        )
        .where(
            docs_sales.c.is_deleted.is_not(True),
            docs_sales.c.cashbox == user.cashbox_id,
            docs_sales_delivery_info.c.delivery_date >= start_of_day,
            docs_sales_delivery_info.c.delivery_date <= end_of_day
        )
    )

    filters_dict = filters.dict(exclude_none=True)
    filter_list = []

    # Фильтрация по конкретному сборщику
    if "picker_id" in filters_dict:
        pid = filters_dict["picker_id"]
        # предполагается, что docs_sales_links.c.user_id содержит ID сотрудника
        picker_exists = exists().where(
            and_(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.picker,
            )
        )
        filter_list.append(picker_exists)

    # Фильтрация по конкретному курьеру/логисту
    if "courier_id" in filters_dict:
        cid = filters_dict["courier_id"]
        courier_exists = exists().where(
            and_(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.courier,
            )
        )
        filter_list.append(courier_exists)

    if "delivery_date_from" in filters_dict or "delivery_date_to" in filters_dict:
        # Присоединяем таблицу с информацией о доставке
        query = query.outerjoin(
            docs_sales_delivery_info,
            docs_sales_delivery_info.c.docs_sales_id == docs_sales.c.id
        )

        # Фильтр по началу периода доставки
        if "delivery_date_from" in filters_dict:
            filter_list.append(
                docs_sales_delivery_info.c.delivery_date >= datetime.datetime.fromtimestamp(filters_dict["delivery_date_from"])
            )

        # Фильтр по концу периода доставки
        if "delivery_date_to" in filters_dict:
            filter_list.append(
                docs_sales_delivery_info.c.delivery_date <= datetime.datetime.fromtimestamp(filters_dict["delivery_date_to"])
            )

    # Обработка has_delivery
    if "has_delivery" in filters_dict:
        address_valid = and_(
            docs_sales_delivery_info.c.address.isnot(None),
            func.trim(cast(docs_sales_delivery_info.c.address, String)) != ''
        )

        note_valid = and_(
            docs_sales_delivery_info.c.note.isnot(None),
            func.trim(cast(docs_sales_delivery_info.c.note, String)) != ''
        )

        # delivery_date: если у вас timestamp -> просто isnot(None); 
        # если у вас unix-int и 0 означает "пусто" — добавьте != 0
        delivery_date_valid = docs_sales_delivery_info.c.delivery_date.isnot(None)

        # recipient: приводим к тексту и отсеиваем '{}' / 'null' / пустую строку
        recipient_text = func.trim(cast(docs_sales_delivery_info.c.recipient, String))
        recipient_valid = and_(
            docs_sales_delivery_info.c.recipient.isnot(None),
            recipient_text != '',
            recipient_text != '{}',
            recipient_text != 'null'
        )

        delivery_any_valid = or_(
            address_valid,
            note_valid,
            delivery_date_valid,
            recipient_valid,
        )

        # --- сам exists() ---
        delivery_exists = exists().where(
            and_(
                docs_sales_delivery_info.c.docs_sales_id == docs_sales.c.id,
                delivery_any_valid
            )
        )

        # Вставляем в фильтр
        if filters_dict.get("has_delivery") is True:
            filter_list.append(delivery_exists)
        elif filters_dict.get("has_delivery") is False:
            filter_list.append(~delivery_exists)

    # Обработка has_picker
    if "has_picker" in filters_dict:
        if filters_dict["has_picker"]:
            picker_exists = exists().where(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.picker
            )
            filter_list.append(picker_exists)
        else:
            picker_not_exists = ~exists().where(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.picker
            )
            filter_list.append(picker_not_exists)

    # Обработка has_courier
    if "has_courier" in filters_dict:
        if filters_dict["has_courier"]:
            courier_exists = exists().where(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.courier
            )
            filter_list.append(courier_exists)
        else:
            courier_not_exists = ~exists().where(
                docs_sales_links.c.docs_sales_id == docs_sales.c.id,
                docs_sales_links.c.role == Role.courier
            )
            filter_list.append(courier_not_exists)

    if "order_status" in filters_dict:
        statuses = filters_dict["order_status"].split(",")
        if statuses:
            filter_list.append(docs_sales.c.order_status.in_(statuses))

    if "priority" in filters_dict:
        value = filters_dict["priority"]
        if isinstance(value, dict):
            # Поддержка {">": 5, "<": 10}
            for op, val in value.items():
                if op == "gt": filter_list.append(docs_sales.c.priority > val)
                if op == "lt": filter_list.append(docs_sales.c.priority < val)
        else:
            filter_list.append(docs_sales.c.priority == value)
    for k, v in filters_dict.items():
        if k in ["has_delivery", "has_picker", "has_courier", "priority", "order_status", "delivery_date_from", "delivery_date_to", "picker_id", "courier_id"]:
            continue
        if k.split("_")[-1] == "from":
            dated_from_param_value = func.to_timestamp(v)
            creation_date = func.to_timestamp(docs_sales.c.dated)
            dated_to_param_value = func.to_timestamp(
                filters_dict.get(k.replace("from", "to"))
            )
            filter_list.append(
                and_(
                    dated_from_param_value <= creation_date,
                    creation_date <= dated_to_param_value,
                )
            )

        elif k.split("_")[-1] == "to":
            continue

        elif type(v) is bool:
            filter_list.append(and_(eval(f"docs_sales.c.{k}.is_({v})")))

        elif type(v) is str:
            filter_list.append(
                and_(
                    *list(
                        map(
                            lambda x: eval(
                                f"docs_sales.c.{k}.ilike(f'%{x.strip().lower()}%')"
                            ),
                            v.strip().split(","),
                        )
                    )
                )
            )
        else:
            filter_list.append(and_(eval(f"docs_sales.c.{k} == {v}")))

    query = query.filter(and_(*filter_list))

    count_query = count_query.filter(and_(*filter_list))

    items_db = await database.fetch_all(query)
    count = await database.fetch_val(count_query)

    items_db = [*map(datetime_to_timestamp, items_db)]

    doc_ids = [item["id"] for item in items_db]

    doc_has_loyality = {}

    if doc_ids:
        loyality_query = entity_to_entity.select().where(
            and_(
                entity_to_entity.c.from_entity == 7,  # docs_sales
                entity_to_entity.c.from_id.in_(doc_ids),  # ID документов продаж
                entity_to_entity.c.to_entity == 6,  # loyality_transactions
                entity_to_entity.c.type
                == "docs_sales_loyality_transactions",  # тип связи
                entity_to_entity.c.status.is_(True),
                entity_to_entity.c.delinked.is_not(True),
            )
        )

        loyality_data = await database.fetch_all(loyality_query)

        loyality_data = [dict(record) for record in loyality_data]
        print("loyality_data", loyality_data)

        for record in loyality_data:
            doc_has_loyality[record["from_id"]] = record["to_id"]

    goods_query = docs_sales_goods.select().where(
        docs_sales_goods.c.docs_sales_id.in_(doc_ids)
    )
    goods_data = await database.fetch_all(goods_query)
    goods_map = {}

    for good in goods_data:
        doc_id = good["docs_sales_id"]
        if doc_id not in goods_map:
            goods_map[doc_id] = []
        goods_map[doc_id].append(good)

    # --- предварительная обработка каждого item (delivery info, counts, скидки) ---
    for item in items_db:
        item = await add_delivery_info_to_doc(item)
        goods = goods_map.get(item["id"], [])
        item["nomenclature_count"] = len(goods)
        item["doc_discount"] = round(
            sum(good.get("sum_discounted", 0) or 0 for good in goods), 2
        )

        contragent_id = item.get("contragent")
        item["has_contragent"] = bool(contragent_id)

        if contragent_id:
            query = (
                select(segment_objects.c.segment_id)
                .where(segment_objects.c.object_id == contragent_id,
                       segment_objects.c.object_type == SegmentObjectType.contragents)
                .distinct()
            )
            res = await database.fetch_all(query)
            item["contragent_segments"] = [r["segment_id"] for r in res]

        has_loyality = item["id"] in doc_has_loyality
        item["has_loyality_card"] = has_loyality

        if has_loyality:
            item["color_status"] = "green"
        elif item["has_contragent"]:
            item["color_status"] = "blue"
        else:
            item["color_status"] = "default"

    settings_ids = [item["settings"] for item in items_db]
    settings_query = docs_sales_settings.select().where(
        docs_sales_settings.c.id.in_(settings_ids)
    )
    settings_data = await database.fetch_all(settings_query)
    settings_map = {setting["id"]: setting for setting in settings_data}

    for item in items_db:
        item["settings"] = settings_map.get(item["settings"])

    items_db = await asyncio.gather(*[raschet_oplat(item) for item in items_db])

    items_db = [*map(raschet_oplat, items_db)]
    items_db = [await instance for instance in items_db]

    # --- Пакетная загрузка пользователей для assigned_picker/assigned_courier ---
    # Собираем все уникальные id, которые нужно развернуть
    picker_ids = set()
    courier_ids = set()
    for item in items_db:
        ap = item.get("assigned_picker")
        ac = item.get("assigned_courier")
        # если поле — объект с вложенным id, берем вложенный id
        if isinstance(ap, dict) and ap.get("id"):
            picker_ids.add(ap.get("id"))
        elif isinstance(ap, (int, str)) and str(ap).strip() != "":
            try:
                picker_ids.add(int(ap))
            except Exception:
                # не число — игнорируем
                pass

        if isinstance(ac, dict) and ac.get("id"):
            courier_ids.add(ac.get("id"))
        elif isinstance(ac, (int, str)) and str(ac).strip() != "":
            try:
                courier_ids.add(int(ac))
            except Exception:
                pass

    user_ids = list(picker_ids.union(courier_ids))

    users_map = {}
    if user_ids:
        user_rows = await database.fetch_all(users.select().where(users.c.id.in_(user_ids)))
        # преобразуем в dict для быстрого доступа
        for u in user_rows:
            users_map[u["id"]] = u

    # Заменяем id на объект { id, first_name, last_name } там, где возможно
    for item in items_db:
        ap = item.get("assigned_picker")
        if ap is not None:
            # если уже объект с полями — используем его (но если это число — разворачиваем по users_map)
            if isinstance(ap, dict):
                # может быть уже развёрнутый объект — оставляем как есть
                if not (ap.get("first_name") or ap.get("last_name")) and ap.get("id") in users_map:
                    u = users_map.get(ap.get("id"))
                    if u:
                        item["assigned_picker"] = {
                            "id": u["id"],
                            "first_name": u.get("first_name"),
                            "last_name": u.get("last_name"),
                        }
            else:
                # ap — id (число/строка) — пытаемся развёрнуть
                try:
                    aid = int(ap)
                    u = users_map.get(aid)
                    if u:
                        item["assigned_picker"] = {
                            "id": u["id"],
                            "first_name": u.get("first_name"),
                            "last_name": u.get("last_name"),
                        }
                    else:
                        # если пользователя нет в мапе — оставляем как ID
                        item["assigned_picker"] = ap
                except Exception:
                    item["assigned_picker"] = ap

        ac = item.get("assigned_courier")
        if ac is not None:
            if isinstance(ac, dict):
                if not (ac.get("first_name") or ac.get("last_name")) and ac.get("id") in users_map:
                    u = users_map.get(ac.get("id"))
                    if u:
                        item["assigned_courier"] = {
                            "id": u["id"],
                            "first_name": u.get("first_name"),
                            "last_name": u.get("last_name"),
                        }
            else:
                try:
                    aid = int(ac)
                    u = users_map.get(aid)
                    if u:
                        item["assigned_courier"] = {
                            "id": u["id"],
                            "first_name": u.get("first_name"),
                            "last_name": u.get("last_name"),
                        }
                    else:
                        item["assigned_courier"] = ac
                except Exception:
                    item["assigned_courier"] = ac

    if show_goods:
        for item in items_db:
            item_goods = goods_map.get(item["id"], [])
            item["goods"] = [
                add_nomenclature_name_to_goods(good) for good in item_goods
            ]

    if show_goods:
        for item in items_db:
            query = docs_sales_goods.select().where(
                docs_sales_goods.c.docs_sales_id == item["id"]
            )
            goods_db = await database.fetch_all(query)
            goods_db = [*map(datetime_to_timestamp, goods_db)]

            goods_db = await asyncio.gather(
                *[add_nomenclature_name_to_goods(good) for good in goods_db]
            )

            item["goods"] = goods_db

    return {"result": items_db, "count": count}


async def check_foreign_keys(instance_values, user, exceptions) -> bool:
    if instance_values.get("client") is not None:
        if instance_values["client"] not in contragents_cache:
            try:
                await check_contragent_exists(
                    instance_values["client"], user.cashbox_id
                )
                contragents_cache.add(instance_values["client"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("contragent") is not None:
        if instance_values["contragent"] not in contragents_cache:
            try:
                await check_contragent_exists(
                    instance_values["contragent"], user.cashbox_id
                )
                contragents_cache.add(instance_values["contragent"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("contract") is not None:
        if instance_values["contract"] not in contracts_cache:
            try:
                await check_entity_exists(
                    contracts, instance_values["contract"], user.id
                )
                contracts_cache.add(instance_values["contract"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("organization") is not None:
        if instance_values["organization"] not in organizations_cache:
            try:
                await check_entity_exists(
                    organizations, instance_values["organization"], user.id
                )
                organizations_cache.add(instance_values["organization"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("warehouse") is not None:
        if instance_values["warehouse"] not in warehouses_cache:
            try:
                await check_entity_exists(
                    warehouses, instance_values["warehouse"], user.id
                )
                warehouses_cache.add(instance_values["warehouse"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("sales_manager") is not None:
        if instance_values["sales_manager"] not in users_cache:
            query = users_cboxes_relation.select().where(
                users_cboxes_relation.c.id == instance_values["sales_manager"]
            )
            if not await database.fetch_one(query):
                exceptions.append(str(instance_values) + " Пользователь не существует!")
                return False
            users_cache.add(instance_values["sales_manager"])
    return True


async def create(
    token: str, docs_sales_data: schemas.CreateMass, generate_out: bool = True
):
    """Создание документов"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    exceptions = []

    count_query = select(func.count(docs_sales.c.id)).where(
        docs_sales.c.cashbox == user.cashbox_id, docs_sales.c.is_deleted.is_(False)
    )
    count_docs_sales = await database.fetch_val(count_query, column=0)

    paybox_q = pboxes.select().where(pboxes.c.cashbox == user.cashbox_id)
    paybox = await database.fetch_one(paybox_q)
    paybox_id = None if not paybox else paybox.id

    article_q = articles.select().where(
        articles.c.cashbox == user.cashbox_id, articles.c.name == "Продажи"
    )
    article_db = await database.fetch_one(article_q)

    for index, instance_values in enumerate(docs_sales_data.dict()["__root__"]):
        instance_values["created_by"] = user.id
        instance_values["sales_manager"] = user.id
        instance_values["is_deleted"] = False
        instance_values["cashbox"] = user.cashbox_id
        instance_values["settings"] = await add_settings_docs_sales(
            instance_values.pop("settings", None)
        )
        priority = instance_values.get("priority")
        if priority is not None and (priority < 0 or priority > 10):
            raise HTTPException(400, "Приоритет должен быть от 0 до 10")

        goods: Union[list, None] = instance_values.pop("goods", None)

        goods_tmp = goods

        paid_rubles = instance_values.pop("paid_rubles", 0)
        paid_rubles = 0 if not paid_rubles else paid_rubles

        paid_lt = instance_values.pop("paid_lt", 0)
        paid_lt = 0 if not paid_lt else paid_lt

        lt = instance_values.pop("loyality_card_id")

        if not await check_period_blocked(
            instance_values["organization"], instance_values.get("dated"), exceptions
        ):
            continue

        if not await check_foreign_keys(
            instance_values,
            user,
            exceptions,
        ):
            continue

        del instance_values["client"]

        if not instance_values.get("number"):
            query = (
                select(docs_sales.c.number)
                .where(
                    docs_sales.c.is_deleted == False,
                    docs_sales.c.organization == instance_values["organization"],
                )
                .order_by(desc(docs_sales.c.created_at))
            )
            prev_number_docs_sales = await database.fetch_one(query)
            if prev_number_docs_sales:
                if prev_number_docs_sales.number:
                    try:
                        number_int = int(prev_number_docs_sales.number)
                    except:
                        number_int = 0
                    instance_values["number"] = str(number_int + 1)
                else:
                    instance_values["number"] = "1"
            else:
                instance_values["number"] = "1"

        paybox = instance_values.pop("paybox", None)
        if paybox is None:
            if paybox_id is not None:
                paybox = paybox_id

        query = docs_sales.insert().values(instance_values)
        instance_id = await database.execute(query)

        # Генерация ссылок для заказа
        try:
            await generate_and_save_order_links(instance_id)
        except Exception as e:
            print(f"Ошибка при генерации ссылок для заказа {instance_id}: {e}")

        # Процесс разделения тегов(в другую таблицу)
        tags = instance_values.pop("tags", "")
        if tags:
            tags_insert_list = []
            tags_split = tags.split(",")
            for tag_name in tags_split:
                tags_insert_list.append(
                    {
                        "docs_sales_id": instance_id,
                        "name": tag_name,
                    }
                )
            if tags_insert_list:
                await database.execute(docs_sales_tags.insert(tags_insert_list))

        inserted_ids.add(instance_id)
        items_sum = 0

        cashback_sum = 0

        lcard = None
        if lt:
            lcard_q = loyality_cards.select().where(loyality_cards.c.id == lt)
            lcard = await database.fetch_one(lcard_q)

        for item in goods:
            item["docs_sales_id"] = instance_id
            del item["nomenclature_name"]
            del item["unit_name"]

            if item.get("price_type") is not None:
                if item["price_type"] not in price_types_cache:
                    try:
                        await check_entity_exists(
                            price_types, item["price_type"], user.id
                        )
                        price_types_cache.add(item["price_type"])
                    except HTTPException as e:
                        exceptions.append(str(item) + " " + e.detail)
                        continue
            if item.get("unit") is not None:
                if item["unit"] not in units_cache:
                    try:
                        await check_unit_exists(item["unit"])
                        units_cache.add(item["unit"])
                    except HTTPException as e:
                        exceptions.append(str(item) + " " + e.detail)
                        continue
            item["nomenclature"] = int(item["nomenclature"])
            query = docs_sales_goods.insert().values(item)
            await database.execute(query)

            items_sum += item["price"] * item["quantity"]

            if lcard:
                nomenclature_db = await database.fetch_one(
                    nomenclature.select().where(
                        nomenclature.c.id == item["nomenclature"]
                    )
                )
                calculated_share = paid_rubles / (paid_rubles + paid_lt)
                if nomenclature_db:
                    if (
                        nomenclature_db.cashback_type
                        == NomenclatureCashbackType.no_cashback
                    ):
                        pass
                    elif (
                        nomenclature_db.cashback_type
                        == NomenclatureCashbackType.percent
                    ):
                        current_percent = (
                            item["price"]
                            * item["quantity"]
                            * (nomenclature_db.cashback_value / 100)
                        )
                        cashback_sum += calculated_share * current_percent
                    elif (
                        nomenclature_db.cashback_type == NomenclatureCashbackType.const
                    ):
                        cashback_sum += (
                            item["quantity"] * nomenclature_db.cashback_value
                        )
                    elif (
                        nomenclature_db.cashback_type
                        == NomenclatureCashbackType.lcard_cashback
                    ):
                        current_percent = (
                            item["price"]
                            * item["quantity"]
                            * (lcard.cashback_percent / 100)
                        )
                        print(current_percent)
                        print(lcard.cashback_percent)
                        print(calculated_share)
                        print(calculated_share * current_percent)
                        cashback_sum += calculated_share * current_percent
                    else:
                        current_percent = (
                            item["price"]
                            * item["quantity"]
                            * (lcard.cashback_percent / 100)
                        )
                        cashback_sum += calculated_share * current_percent
                else:
                    current_percent = (
                        item["price"]
                        * item["quantity"]
                        * (lcard.cashback_percent / 100)
                    )
                    cashback_sum += calculated_share * current_percent

            if instance_values.get("warehouse") is not None:
                query = (
                    warehouse_balances.select()
                    .where(
                        warehouse_balances.c.warehouse_id
                        == instance_values["warehouse"],
                        warehouse_balances.c.nomenclature_id == item["nomenclature"],
                    )
                    .order_by(desc(warehouse_balances.c.created_at))
                )
                last_warehouse_balance = await database.fetch_one(query)
                warehouse_amount = (
                    last_warehouse_balance.current_amount
                    if last_warehouse_balance
                    else 0
                )

                query = warehouse_balances.insert().values(
                    {
                        "organization_id": instance_values["organization"],
                        "warehouse_id": instance_values["warehouse"],
                        "nomenclature_id": item["nomenclature"],
                        "document_sale_id": instance_id,
                        "outgoing_amount": item["quantity"],
                        "current_amount": warehouse_amount - item["quantity"],
                        "cashbox_id": user.cashbox_id,
                    }
                )
                await database.execute(query)

        if paid_rubles > 0:
            if article_db:
                article_id = article_db.id
            else:
                tstamp = int(datetime.datetime.now().timestamp())
                created_article_q = articles.insert().values(
                    {
                        "name": "Продажи",
                        "emoji": "🛍️",
                        "cashbox": user.cashbox_id,
                        "created_at": tstamp,
                        "updated_at": tstamp,
                    }
                )
                article_id = await database.execute(created_article_q)

            payment_id = await database.execute(
                payments.insert().values(
                    {
                        "contragent": instance_values["contragent"],
                        "type": "incoming",
                        "name": f"Оплата по документу {instance_values['number']}",
                        "amount_without_tax": round(paid_rubles, 2),
                        "tags": tags,
                        "amount": round(paid_rubles, 2),
                        "tax": 0,
                        "tax_type": "internal",
                        "article_id": article_id,
                        "article": "Продажи",
                        "paybox": paybox,
                        "date": int(datetime.datetime.now().timestamp()),
                        "account": user.user,
                        "cashbox": user.cashbox_id,
                        "is_deleted": False,
                        "created_at": int(datetime.datetime.now().timestamp()),
                        "updated_at": int(datetime.datetime.now().timestamp()),
                        "status": instance_values["status"],
                        "stopped": True,
                        "docs_sales_id": instance_id,
                    }
                )
            )
            await database.execute(
                pboxes.update()
                .where(pboxes.c.id == paybox)
                .values({"balance": pboxes.c.balance - paid_rubles})
            )

            # Юкасса

            yookassa_oauth_service = OauthService(
                oauth_repository=YookassaOauthRepository(),
                request_repository=YookassaRequestRepository(),
                get_oauth_credential_function=GetOauthCredentialFunction(),
            )

            yookassa_api_service = YookassaApiService(
                request_repository=YookassaRequestRepository(),
                oauth_repository=YookassaOauthRepository(),
                payments_repository=YookassaPaymentsRepository(),
                crm_payments_repository=YookassaCrmPaymentsRepository(),
                table_nomenclature_repository=YookassaTableNomenclature(),
                amo_table_crm_repository=YookasssaAmoTableCrmRepository(),
            )

            if await yookassa_oauth_service.validation_oauth(
                user.cashbox_id, instance_values["warehouse"]
            ):
                await yookassa_api_service.api_create_payment(
                    user.cashbox_id,
                    instance_values["warehouse"],
                    instance_id,
                    payment_id,
                    PaymentCreateModel(
                        amount=AmountModel(
                            value=str(round(paid_rubles, 2)), currency="RUB"
                        ),
                        description=f"Оплата по документу {instance_values['number']}",
                        capture=True,
                        receipt=ReceiptModel(
                            customer=CustomerModel(),
                            items=[
                                ItemModel(
                                    description=good.get("nomenclature_name") or "",
                                    amount=AmountModel(
                                        value=good.get("price"), currency="RUB"
                                    ),
                                    quantity=good.get("quantity"),
                                    vat_code="1",
                                )
                                for good in goods_tmp
                            ],
                        ),
                        confirmation=ConfirmationRedirect(
                            type="redirect",
                            return_url=f"https://${os.getenv('APP_URL')}/?token=${token}",
                        ),
                    ),
                )

            # юкасса

            await database.execute(
                entity_to_entity.insert().values(
                    {
                        "from_entity": 7,
                        "to_entity": 5,
                        "cashbox_id": user.cashbox_id,
                        "type": "docs_sales_payments",
                        "from_id": instance_id,
                        "to_id": payment_id,
                        "status": True,
                        "delinked": False,
                    }
                )
            )
            if lcard:
                if cashback_sum > 0:
                    calculated_cashback_sum = round((cashback_sum), 2)
                    if calculated_cashback_sum > 0:
                        rubles_body = {
                            "loyality_card_id": lt,
                            "loyality_card_number": lcard.card_number,
                            "type": "accrual",
                            "name": f"Кешбек по документу {instance_values['number']}",
                            "amount": calculated_cashback_sum,
                            "created_by_id": user.id,
                            "tags": tags,
                            "card_balance": lcard.balance,
                            "dated": datetime.datetime.now(),
                            "cashbox": user.cashbox_id,
                            "is_deleted": False,
                            "created_at": datetime.datetime.now(),
                            "updated_at": datetime.datetime.now(),
                            "status": True,
                        }

                        lt_id = await database.execute(
                            loyality_transactions.insert().values(rubles_body)
                        )

                        await asyncio.gather(asyncio.create_task(raschet_bonuses(lt)))

            await asyncio.gather(asyncio.create_task(raschet(user, token)))
        if lt:
            if paid_lt > 0:
                paybox_q = loyality_cards.select().where(loyality_cards.c.id == lt)
                payboxes = await database.fetch_one(paybox_q)
                print("loyality_transactions insert")
                rubles_body = {
                    "loyality_card_id": lt,
                    "loyality_card_number": payboxes.card_number,
                    "type": "withdraw",
                    "name": f"Оплата по документу {instance_values['number']}",
                    "amount": paid_lt,
                    "created_by_id": user.id,
                    "card_balance": lcard.balance,
                    "tags": tags,
                    "dated": datetime.datetime.now(),
                    "cashbox": user.cashbox_id,
                    "is_deleted": False,
                    "created_at": datetime.datetime.now(),
                    "updated_at": datetime.datetime.now(),
                    "status": True,
                }
                print("loyality_transactions insert")
                lt_id = await database.execute(
                    loyality_transactions.insert().values(rubles_body)
                )
                print("loyality_transactions insert")
                await database.execute(
                    loyality_cards.update()
                    .where(loyality_cards.c.card_number == payboxes.card_number)
                    .values({"balance": loyality_cards.c.balance - paid_lt})
                )
                print("loyality_transactions update")
                await database.execute(
                    entity_to_entity.insert().values(
                        {
                            "from_entity": 7,
                            "to_entity": 6,
                            "cashbox_id": user.cashbox_id,
                            "type": "docs_sales_loyality_transactions",
                            "from_id": instance_id,
                            "to_id": lt_id,
                            "status": True,
                            "delinked": False,
                        }
                    )
                )

                await asyncio.gather(asyncio.create_task(raschet_bonuses(lt)))

        query = (
            docs_sales.update()
            .where(docs_sales.c.id == instance_id)
            .values({"sum": round(items_sum, 2)})
        )
        await database.execute(query)

        if generate_out:
            goods_res = []
            for good in goods:
                nomenclature_id = int(good["nomenclature"])
                nomenclature_db = await database.fetch_one(
                    nomenclature.select().where(nomenclature.c.id == nomenclature_id)
                )
                if nomenclature_db.type == "product":
                    goods_res.append(
                        {
                            "price_type": 1,
                            "price": 0,
                            "quantity": good["quantity"],
                            "unit": good["unit"],
                            "nomenclature": nomenclature_id,
                        }
                    )

            body = {
                "number": None,
                "dated": instance_values["dated"],
                "docs_purchases": None,
                "to_warehouse": None,
                "status": True,
                "contragent": instance_values["contragent"],
                "organization": instance_values["organization"],
                "operation": "outgoing",
                "comment": instance_values["comment"],
                "warehouse": instance_values["warehouse"],
                "docs_sales_id": instance_id,
                "goods": goods_res,
            }
            body["docs_purchases"] = None
            body["number"] = None
            body["to_warehouse"] = None
            await create_warehouse_docs(token, body, user.cashbox_id)

    query = docs_sales.select().where(docs_sales.c.id.in_(inserted_ids))
    docs_sales_db = await database.fetch_all(query)
    docs_sales_db = [*map(datetime_to_timestamp, docs_sales_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "docs_sales",
            "result": docs_sales_db,
        },
    )

    if exceptions:
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return docs_sales_db


@router.patch("/docs_sales/{idx}/", response_model=schemas.ListView)
async def update(token: str, docs_sales_data: schemas.EditMass):
    """Редактирование документов"""
    user = await get_user_by_token(token)

    updated_ids = set()
    exceptions = []

    count_query = select(func.count(docs_sales.c.id)).where(
        docs_sales.c.cashbox == user.cashbox_id, docs_sales.c.is_deleted.is_(False)
    )

    count_docs_sales = await database.fetch_val(count_query, column=0)

    for index, instance_values in enumerate(
        docs_sales_data.dict(exclude_unset=True)["__root__"]
    ):
        if not await check_period_blocked(
            instance_values["organization"], instance_values.get("dated"), exceptions
        ):
            continue

        if not await check_foreign_keys(instance_values, user, exceptions):
            continue

        # if instance_values.get("number") is None:
        #     instance_values["number"] = str(count_docs_sales + index + 1)

        goods: Union[list, None] = instance_values.pop("goods", None)

        paid_rubles = instance_values.pop("paid_rubles", 0)
        paid_lt = instance_values.pop("paid_lt", 0)
        lt = instance_values.pop("loyality_card_id", None)

        paybox = instance_values.pop("paybox", None)
        if paybox is None:
            paybox_q = pboxes.select().where(pboxes.c.cashbox == user.cashbox_id)
            paybox = await database.fetch_one(paybox_q)
            if paybox:
                paybox = paybox.id

        instance_id_db = instance_values["id"]

        settings: Optional[Dict[str, Any]] = instance_values.pop("settings", None)
        if await exists_settings_docs_sales(instance_id_db):
            await update_settings_docs_sales(instance_id_db, settings)
        else:
            instance_values["settings"] = await add_settings_docs_sales(settings)

        if paid_rubles or paid_lt or lt:
            query = entity_to_entity.select().where(
                entity_to_entity.c.cashbox_id == user.cashbox_id,
                entity_to_entity.c.from_id == instance_values["id"],
            )
            proxyes = await database.fetch_all(query)

            proxy_payment = False
            proxy_lt = False

            for proxy in proxyes:
                if proxy.from_entity == 7:
                    # Платеж

                    if proxy.to_entity == 5:
                        q_payment = (
                            payments.update()
                            .where(
                                payments.c.id == proxy.to_id,
                                payments.c.cashbox == user.cashbox_id,
                                payments.c.status == True,
                                payments.c.is_deleted == False,
                            )
                            .values(
                                {
                                    "amount": paid_rubles,
                                    "amount_without_tax": paid_rubles,
                                }
                            )
                        )
                        await database.execute(q_payment)
                        proxy_payment = True

                    # Транзакция
                    if proxy.to_entity == 6:
                        q_trans = (
                            loyality_transactions.update()
                            .where(
                                loyality_transactions.c.id == proxy.to_id,
                                loyality_transactions.c.cashbox == user.cashbox_id,
                                loyality_transactions.c.status == True,
                                loyality_transactions.c.is_deleted == False,
                            )
                            .values({"amount": paid_lt})
                        )
                        await database.execute(q_trans)
                        proxy_lt = True

            if not proxy_payment:
                rubles_body = {
                    "contragent": instance_values["contragent"],
                    "type": "outgoing",
                    "name": f"Оплата по документу {instance_values['number']}",
                    "amount_without_tax": instance_values.get("paid_rubles"),
                    "amount": instance_values.get("paid_rubles"),
                    "paybox": paybox,
                    "tags": instance_values.get("tags", ""),
                    "date": int(datetime.datetime.now().timestamp()),
                    "account": user.user,
                    "cashbox": user.cashbox_id,
                    "is_deleted": False,
                    "created_at": int(datetime.datetime.now().timestamp()),
                    "updated_at": int(datetime.datetime.now().timestamp()),
                    "status": True,
                    "stopped": True,
                    "docs_sales_id": instance_id_db,
                }
                payment_id = await database.execute(
                    payments.insert().values(rubles_body)
                )

                await database.execute(
                    entity_to_entity.insert().values(
                        {
                            "from_entity": 7,
                            "to_entity": 5,
                            "cashbox_id": user.cashbox_id,
                            "type": "docs_sales_payments",
                            "from_id": instance_id_db,
                            "to_id": payment_id,
                            "status": True,
                            "delinked": False,
                        }
                    )
                )

                if lt:
                    lcard_q = loyality_cards.select().where(loyality_cards.c.id == lt)
                    lcard = await database.fetch_one(lcard_q)
                    rubles_body = {
                        "loyality_card_id": lt,
                        "loyality_card_number": lcard.card_number,
                        "type": "accrual",
                        "name": f"Кешбек по документу {instance_values['number']}",
                        "amount": round(
                            (paid_rubles * (lcard.cashback_value / 100)), 2
                        ),
                        "created_by_id": user.id,
                        "card_balance": lcard.balance,
                        "tags": instance_values.get("tags", ""),
                        "dated": datetime.datetime.now(),
                        "cashbox": user.cashbox_id,
                        "is_deleted": False,
                        "created_at": datetime.datetime.now(),
                        "updated_at": datetime.datetime.now(),
                        "status": True,
                    }
                    lt_id = await database.execute(
                        loyality_transactions.insert().values(rubles_body)
                    )
                    await asyncio.gather(asyncio.create_task(raschet_bonuses(lt)))

                await asyncio.gather(asyncio.create_task(raschet(user, token)))

            if lt and not proxy_lt:
                if paid_lt > 0:
                    paybox_q = loyality_cards.select().where(loyality_cards.c.id == lt)
                    payboxes = await database.fetch_one(paybox_q)
                    lcard_q = loyality_cards.select().where(loyality_cards.c.id == lt)
                    lcard = await database.fetch_one(lcard_q)

                    rubles_body = {
                        "loyality_card_id": lt,
                        "loyality_card_number": payboxes.card_number,
                        "type": "withdraw",
                        "name": f"Оплата по документу {instance_values['number']}",
                        "amount": paid_lt,
                        "created_by_id": user.id,
                        "tags": instance_values.get("tags", ""),
                        "dated": datetime.datetime.now(),
                        "card_balance": lcard.balance,
                        "cashbox": user.cashbox_id,
                        "is_deleted": False,
                        "created_at": datetime.datetime.now(),
                        "updated_at": datetime.datetime.now(),
                        "status": True,
                    }
                    lt_id = await database.execute(
                        loyality_transactions.insert().values(rubles_body)
                    )

                    await database.execute(
                        entity_to_entity.insert().values(
                            {
                                "from_entity": 7,
                                "to_entity": 6,
                                "cashbox_id": user.cashbox_id,
                                "type": "docs_sales_loyality_transactions",
                                "from_id": instance_id_db,
                                "to_id": lt_id,
                                "status": True,
                                "delinked": False,
                            }
                        )
                    )

                    await asyncio.gather(asyncio.create_task(raschet_bonuses(lt)))

        if instance_values.get("paid_rubles"):
            del instance_values["paid_rubles"]

        query = (
            docs_sales.update()
            .where(docs_sales.c.id == instance_values["id"])
            .values(instance_values)
        )
        await database.execute(query)
        instance_id = instance_values["id"]
        updated_ids.add(instance_id)
        if goods:
            query = docs_sales_goods.delete().where(
                docs_sales_goods.c.docs_sales_id == instance_id
            )
            await database.execute(query)
            items_sum = 0
            for item in goods:
                item["docs_sales_id"] = instance_id

                if item.get("price_type") is not None:
                    if item["price_type"] not in price_types_cache:
                        try:
                            await check_entity_exists(
                                price_types, item["price_type"], user.id
                            )
                            price_types_cache.add(item["price_type"])
                        except HTTPException as e:
                            exceptions.append(str(item) + " " + e.detail)
                            continue
                if item.get("unit") is not None:
                    if item["unit"] not in units_cache:
                        try:
                            await check_unit_exists(item["unit"])
                            units_cache.add(item["unit"])
                        except HTTPException as e:
                            exceptions.append(str(item) + " " + e.detail)
                            continue
                item["nomenclature"] = int(item["nomenclature"])
                query = docs_sales_goods.insert().values(item)
                await database.execute(query)
                items_sum += item["price"] * item["quantity"]
                if instance_values.get("warehouse") is not None:
                    query = (
                        warehouse_balances.select()
                        .where(
                            warehouse_balances.c.warehouse_id
                            == instance_values["warehouse"],
                            warehouse_balances.c.nomenclature_id
                            == item["nomenclature"],
                        )
                        .order_by(desc(warehouse_balances.c.created_at))
                    )
                    last_warehouse_balance = await database.fetch_one(query)
                    warehouse_amount = (
                        last_warehouse_balance.current_amount
                        if last_warehouse_balance
                        else 0
                    )

                    query = warehouse_balances.insert().values(
                        {
                            "organization_id": instance_values["organization"],
                            "warehouse_id": instance_values["warehouse"],
                            "nomenclature_id": item["nomenclature"],
                            "document_sale_id": instance_id,
                            "outgoing_amount": item["quantity"],
                            "current_amount": warehouse_amount - item["quantity"],
                            "cashbox_id": user.cashbox_id,
                        }
                    )
                    await database.execute(query)

            query = (
                docs_sales.update()
                .where(docs_sales.c.id == instance_id)
                .values({"sum": round(items_sum, 2)})
            )
            await database.execute(query)

            doc_warehouse = await database.fetch_one(
                docs_warehouse.select()
                .where(docs_warehouse.c.docs_sales_id == instance_id)
                .order_by(desc(docs_warehouse.c.id))
            )

            goods_res = []
            for good in goods:
                nomenclature_db = await database.fetch_one(
                    nomenclature.select().where(
                        nomenclature.c.id == good["nomenclature"]
                    )
                )
                if nomenclature_db.type == "product":
                    goods_res.append(
                        {
                            "price_type": 1,
                            "price": 0,
                            "quantity": good["quantity"],
                            "unit": good["unit"],
                            "nomenclature": good["nomenclature"],
                        }
                    )

            body = WarehouseUpdate(
                __root__=[
                    {
                        "id": doc_warehouse.id,
                        "number": None,
                        "dated": instance_values.get("dated"),
                        "docs_purchases": None,
                        "to_warehouse": None,
                        "status": True,
                        "contragent": instance_values["contragent"],
                        "operation": "outgoing",
                        "comment": instance_values["comment"],
                        "warehouse": instance_values["warehouse"],
                        "docs_sales_id": instance_id,
                        "goods": goods_res,
                    }
                ]
            )

            await update_warehouse_doc(token, body)

    query = docs_sales.select().where(docs_sales.c.id.in_(updated_ids))
    docs_sales_db = await database.fetch_all(query)
    docs_sales_db = [*map(datetime_to_timestamp, docs_sales_db)]

    await manager.send_message(
        token,
        {
            "action": "edit",
            "target": "docs_sales",
            "result": docs_sales_db,
        },
    )

    if exceptions:
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return docs_sales_db


@router.delete("/docs_sales/", response_model=schemas.ListView)
async def delete(token: str, ids: list[int]):
    """Пакетное удаление документов"""
    await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id.in_(ids), docs_sales.c.is_deleted.is_not(True)
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if items_db:
        query = (
            docs_sales.update()
            .where(docs_sales.c.id.in_(ids), docs_sales.c.is_deleted.is_not(True))
            .values({"is_deleted": True})
        )
        await database.execute(query)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "docs_sales",
                "result": items_db,
            },
        )

    return items_db


@router.delete("/docs_sales/{idx}/", response_model=schemas.ListView)
async def delete(token: str, idx: int):
    """Удаление документа"""
    await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id == idx, docs_sales.c.is_deleted.is_not(True)
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if items_db:
        query = (
            docs_sales.update()
            .where(docs_sales.c.id == idx, docs_sales.c.is_deleted.is_not(True))
            .values({"is_deleted": True})
        )
        await database.execute(query)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "docs_sales",
                "result": items_db,
            },
        )

    return items_db


@router.post(
    "/docs_sales/{idx}/delivery_info/",
    response_model=schemas.ResponseDeliveryInfoSchema,
)
async def delivery_info(token: str, idx: int, data: schemas.DeliveryInfoSchema):
    """Добавление информации о доставке в заказу"""

    user = await get_user_by_token(token)

    check_query = select(docs_sales.c.id).where(
        and_(
            docs_sales.c.id == idx,
            docs_sales.c.cashbox == user.cashbox_id,
            docs_sales.c.is_deleted == False,
        )
    )

    item_db = await database.fetch_one(check_query)
    if not item_db:
        raise HTTPException(404, "Документ не найден!")

    check_delivery_info_query = select(docs_sales_delivery_info.c.id).where(
        docs_sales_delivery_info.c.docs_sales_id == idx
    )
    delivery_info_db = await database.fetch_one(check_delivery_info_query)
    if delivery_info_db:
        raise HTTPException(400, "Данные доставки уже добавлены.")

    data_dict = data.dict()
    data_dict["docs_sales_id"] = idx
    if data_dict.get("delivery_date") or data_dict.get("delivery_date") == 0:
        data_dict["delivery_date"] = datetime.datetime.fromtimestamp(
            data_dict["delivery_date"]
        )
    insert_query = docs_sales_delivery_info.insert().values(data_dict)
    inserted_entity_id = await database.execute(insert_query)

    return schemas.ResponseDeliveryInfoSchema(
        id=inserted_entity_id, docs_sales_id=idx, **data.dict()
    )


@router.get("/docs_sales/{idx}/links", response_model=schemas.OrderLinksResponse)
async def get_order_links(token: str, idx: int):
    """Получение сгенерированных ссылок для заказа"""
    user = await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id == idx,
        docs_sales.c.is_deleted.is_not(True),
        docs_sales.c.cashbox == user.cashbox_id,
    )
    order = await database.fetch_one(query)

    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")

    links_data = await generate_and_save_order_links(idx)

    if not links_data:
        raise HTTPException(status_code=500, detail="Не удалось сгенерировать ссылки")

    return schemas.OrderLinksResponse(**links_data)


@router.post("/docs_sales/{idx}/notify", response_model=schemas.NotifyResponse)
async def notify_order(
    token: str, idx: int, notify_config: schemas.NotifyConfig = Depends()
):
    """Генерация и отправка уведомлений о заказе"""
    user = await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id == idx,
        docs_sales.c.is_deleted.is_not(True),
        docs_sales.c.cashbox == user.cashbox_id,
    )
    order = await database.fetch_one(query)

    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")

    order_data = dict(order)

    query = docs_sales_goods.select().where(docs_sales_goods.c.docs_sales_id == idx)
    goods_db = await database.fetch_all(query)
    goods_data = [dict(good) for good in goods_db]

    contragent_data = {}
    if order.contragent:
        query = contragents.select().where(contragents.c.id == order.contragent)
        contragent = await database.fetch_one(query)
        if contragent:
            contragent_data = dict(contragent)

    delivery_info = None
    query = docs_sales_delivery_info.select().where(
        docs_sales_delivery_info.c.docs_sales_id == idx
    )
    delivery = await database.fetch_one(query)
    if delivery:
        delivery_info = dict(delivery)

    links_data = await generate_and_save_order_links(idx)

    if not links_data:
        raise HTTPException(status_code=500, detail="Не удалось сгенерировать ссылки")

    hashes = {
        "general": links_data["general_link"]["hash"],
        "picker": links_data["picker_link"]["hash"],
        "courier": links_data["courier_link"]["hash"],
    }

    links = {
        "general_url": links_data["general_link"]["url"],
        "picker_url": links_data["picker_link"]["url"],
        "courier_url": links_data["courier_link"]["url"],
    }

    notify_type_str = notify_config.type.value

    notification_text = format_notification_text(
        notification_type=notify_type_str,
        order_data=order_data,
        goods_data=goods_data,
        contragent_data=contragent_data,
        delivery_info=delivery_info,
        links=links,
        hashes=hashes,
    )

    recipients = []

    if notify_config.type == schemas.NotifyType.assembly:
        if order.assigned_picker:
            if await check_user_on_shift(order.assigned_picker, check_shift_settings=True):
                picker_query = (
                    select([users.c.chat_id])
                    .select_from(
                        users.join(
                            users_cboxes_relation,
                            users.c.id == users_cboxes_relation.c.user,
                        )
                    )
                    .where(users_cboxes_relation.c.id == order.assigned_picker)
                )
                picker = await database.fetch_one(picker_query)
                if picker and picker.chat_id:
                    recipients.append(picker.chat_id)
        
        if not recipients:
            available_pickers = await get_available_pickers_on_shift(order.cashbox)
            
            if available_pickers:
                pickers_query = (
                    select([users.c.chat_id])
                    .select_from(
                        users.join(
                            users_cboxes_relation,
                            users.c.id == users_cboxes_relation.c.user,
                        )
                    )
                    .where(users_cboxes_relation.c.id.in_(available_pickers))
                )
                pickers = await database.fetch_all(pickers_query)
                for picker in pickers:
                    if picker.chat_id:
                        recipients.append(picker.chat_id)

    elif notify_config.type == schemas.NotifyType.delivery:
        if order.assigned_courier:
            if await check_user_on_shift(order.assigned_courier, check_shift_settings=True):
                courier_query = (
                    select([users.c.chat_id])
                    .select_from(
                        users.join(
                            users_cboxes_relation,
                            users.c.id == users_cboxes_relation.c.user,
                        )
                    )
                    .where(users_cboxes_relation.c.id == order.assigned_courier)
                )
                courier = await database.fetch_one(courier_query)
                if courier and courier.chat_id:
                    recipients.append(courier.chat_id)
        
        if not recipients:
            available_couriers = await get_available_couriers_on_shift(order.cashbox)
            
            if available_couriers:
                couriers_query = (
                    select([users.c.chat_id])
                    .select_from(
                        users.join(
                            users_cboxes_relation,
                            users.c.id == users_cboxes_relation.c.user,
                        )
                    )
                    .where(users_cboxes_relation.c.id.in_(available_couriers))
                )
                couriers = await database.fetch_all(couriers_query)
                for courier in couriers:
                    if courier.chat_id:
                        recipients.append(courier.chat_id)

    elif notify_config.type == schemas.NotifyType.general:
        if order.assigned_picker and await check_user_on_shift(order.assigned_picker, check_shift_settings=True):
            picker_query = (
                select([users.c.chat_id])
                .select_from(
                    users.join(
                        users_cboxes_relation,
                        users.c.id == users_cboxes_relation.c.user,
                    )
                )
                .where(users_cboxes_relation.c.id == order.assigned_picker)
            )
            picker = await database.fetch_one(picker_query)
            if picker and picker.chat_id:
                recipients.append(picker.chat_id)
        
        if order.assigned_courier and await check_user_on_shift(order.assigned_courier, check_shift_settings=True):
            courier_query = (
                select([users.c.chat_id])
                .select_from(
                    users.join(
                        users_cboxes_relation,
                        users.c.id == users_cboxes_relation.c.user,
                    )
                )
                .where(users_cboxes_relation.c.id == order.assigned_courier)
            )
            courier = await database.fetch_one(courier_query)
            if courier and courier.chat_id:
                recipients.append(courier.chat_id)
        
        if not recipients:
            all_available = []
            available_pickers = await get_available_pickers_on_shift(order.cashbox)  # По умолчанию учитывает настройки
            available_couriers = await get_available_couriers_on_shift(order.cashbox)  # По умолчанию учитывает настройки
            all_available.extend(available_pickers)
            all_available.extend(available_couriers)
            all_available = list(set(all_available)) 
            
            if all_available:
                workers_query = (
                    select([users.c.chat_id])
                    .select_from(
                        users.join(
                            users_cboxes_relation,
                            users.c.id == users_cboxes_relation.c.user,
                        )
                    )
                    .where(users_cboxes_relation.c.id.in_(all_available))
                )
                workers = await database.fetch_all(workers_query)
                for worker in workers:
                    if worker.chat_id:
                        recipients.append(worker.chat_id)

    # Если никого не найдено среди работников со сменами - уведомляем админа
    if not recipients:
        owner_query = (
            select([users.c.chat_id])
            .select_from(
                users.join(
                    users_cboxes_relation, users.c.id == users_cboxes_relation.c.user
                )
            )
            .where(
                users_cboxes_relation.c.cashbox_id == order.cashbox,
                users_cboxes_relation.c.is_owner,
            )
        )
        owner = await database.fetch_one(owner_query)
        if owner and owner.chat_id:
            recipients.append(owner.chat_id)

    print(f"Determined recipients: {recipients}")

    if notify_config.type.value == "Общее":
        notify_type_str = "general"
    elif notify_config.type.value == "Сборка":
        notify_type_str = "assembly"
    elif notify_config.type.value == "Доставка":
        notify_type_str = "delivery"
    else:
        notify_type_str = notify_config.type.value

    await send_order_notification(
        notification_type=notify_type_str,
        order_id=idx,
        order_data=order_data,
        recipient_ids=recipients,
        notification_text=notification_text,
        links=links,
    )

    response = {
        "success": True,
        "message": f"Уведомление '{notify_config.type}' сформировано и отправлено",
    }

    if notify_config.type == schemas.NotifyType.general:
        response["general_url"] = links["general_url"]
    elif notify_config.type == schemas.NotifyType.assembly:
        response["picker_url"] = links["picker_url"]
    elif notify_config.type == schemas.NotifyType.delivery:
        response["courier_url"] = links["courier_url"]

    return response


@router.patch("/docs_sales/{idx}/status", response_model=schemas.View)
async def update_order_status(
    token: str, idx: int, status_update: schemas.OrderStatusUpdate
):
    """Обновление статуса заказа"""
    user = await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id == idx,
        docs_sales.c.is_deleted.is_not(True),
        docs_sales.c.cashbox == user.cashbox_id,
    )
    order = await database.fetch_one(query)

    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")

    current_status = order.order_status or OrderStatus.received
    target_status = status_update.status

    valid_transitions = {
        OrderStatus.received: [OrderStatus.processed, OrderStatus.closed],
        OrderStatus.processed: [OrderStatus.collecting, OrderStatus.closed],
        OrderStatus.collecting: [OrderStatus.collected, OrderStatus.closed],
        OrderStatus.collected: [OrderStatus.picked, OrderStatus.closed],
        OrderStatus.picked: [OrderStatus.delivered, OrderStatus.closed],
        OrderStatus.delivered: [OrderStatus.success, OrderStatus.closed],
    }

    if target_status not in valid_transitions.get(current_status, []):
        raise HTTPException(
            status_code=400,
            detail=f"Недопустимый переход статуса с '{current_status}' на '{target_status}'",
        )

    update_values = {"order_status": target_status}

    notification_recipients = []

    # Автоматическое назначение сборщика при переходе в статус "Сборка начата"
    if target_status == OrderStatus.collecting:
        update_values["picker_started_at"] = datetime.datetime.now()
        # Если сборщик еще не назначен, назначаем текущего пользователя
        if not order.assigned_picker:
            update_values["assigned_picker"] = user.id
        
        # Проверяем назначенного сборщика
        assigned_picker = order.assigned_picker or user.id
        if await check_user_on_shift(assigned_picker):
            notification_recipients.append(assigned_picker)
        else:
            # Ищем всех доступных сборщиков на смене
            available_pickers = await get_available_pickers_on_shift(order.cashbox)
            notification_recipients.extend(available_pickers)

    elif target_status == OrderStatus.picked:
        update_values["courier_picked_at"] = datetime.datetime.now()
        # Если курьер еще не назначен, назначаем текущего пользователя
        if not order.assigned_courier:
            update_values["assigned_courier"] = user.id
        
        assigned_courier = order.assigned_courier or user.id
        if await check_user_on_shift(assigned_courier):
            notification_recipients.append(assigned_courier)
        else:
            available_couriers = await get_available_couriers_on_shift(order.cashbox)
            notification_recipients.extend(available_couriers)

    elif target_status == OrderStatus.collected:
        update_values["picker_finished_at"] = datetime.datetime.now()
        if order.assigned_courier:
            if await check_user_on_shift(order.assigned_courier):
                notification_recipients.append(order.assigned_courier)
            else:
                available_couriers = await get_available_couriers_on_shift(order.cashbox)
                notification_recipients.extend(available_couriers)

    elif target_status == OrderStatus.delivered:
        update_values["courier_delivered_at"] = datetime.datetime.now()

    notification_recipients = list(set(notification_recipients))

    if status_update.comment:
        update_values["comment"] = (
            f"{order.comment or ''}\n[{datetime.datetime.now()}] {status_update.comment}"
        )

    query = docs_sales.update().where(docs_sales.c.id == idx).values(update_values)
    await database.execute(query)

    query = docs_sales.select().where(docs_sales.c.id == idx)
    updated_order = await database.fetch_one(query)
    updated_order = datetime_to_timestamp(updated_order)
    updated_order = await raschet_oplat(updated_order)
    updated_order = await add_docs_sales_settings(updated_order)

    # Получаем данные о назначенных пользователях
    if updated_order.get("assigned_picker"):
        user_query = users.select().where(users.c.id == updated_order["assigned_picker"])
        picker_user = await database.fetch_one(user_query)
        if picker_user:
            updated_order["assigned_picker"] = {
                "id": picker_user.id,
                "first_name": picker_user.first_name,
                "last_name": picker_user.last_name
            }
            await manager.send_message(
                token,
                {
                    "action": "assign_user",
                    "target": "docs_sales",
                    "id": idx,
                    "role": "picker",
                    "user_id": picker_user.id,
                },
            )


    if updated_order.get("assigned_courier"):
        user_query = users.select().where(users.c.id == updated_order["assigned_courier"])
        courier_user = await database.fetch_one(user_query)
        if courier_user:
            updated_order["assigned_courier"] = {
                "id": courier_user.id,
                "first_name": courier_user.first_name,
                "last_name": courier_user.last_name
            }
            await manager.send_message(
                token,
                {
                    "action": "assign_user",
                    "target": "docs_sales",
                    "id": idx,
                    "role": "courier",
                    "user_id": courier_user.id,
                },
            )

    query = docs_sales_goods.select().where(docs_sales_goods.c.docs_sales_id == idx)
    goods_db = await database.fetch_all(query)
    goods_db = [*map(datetime_to_timestamp, goods_db)]
    goods_db = [*map(add_nomenclature_name_to_goods, goods_db)]
    goods_db = [await instance for instance in goods_db]

    updated_order["goods"] = goods_db
    updated_order = await add_delivery_info_to_doc(updated_order)

    await manager.send_message(
        token,
        {
            "action": "update_status",
            "target": "docs_sales",
            "id": idx,
            "status": target_status,
        },
    )

    if notification_recipients:
        recipient_chat_ids = []
        for recipient_id in notification_recipients:
            recipient_query = (
                select([users.c.chat_id])
                .select_from(
                    users_cboxes_relation.join(
                        users, users_cboxes_relation.c.user == users.c.id
                    )
                )
                .where(users_cboxes_relation.c.id == recipient_id)
            )
            recipient = await database.fetch_one(recipient_query)
            if recipient and recipient.chat_id:
                recipient_chat_ids.append(recipient.chat_id)

        links_data = await generate_and_save_order_links(idx)

        if not links_data:
            links_data = await generate_and_save_order_links(idx)

        links = {
            "general_url": links_data["general_link"]["url"],
            "picker_url": links_data["picker_link"]["url"],
            "courier_url": links_data["courier_link"]["url"],
        }

        notification_data = {
            "type": "status_change",
            "order_id": idx,
            "previous_status": current_status,
            "status": target_status,
            "recipients": recipient_chat_ids,
            "links": links,
            "updated_by": user.id,
            "updated_at": datetime.datetime.now().timestamp(),
        }

        await queue_notification(notification_data)

    return updated_order


@router.patch("/docs_sales/{idx}/assign/{role}", response_model=schemas.View)
async def assign_user_to_order(token: str, idx: int, role: schemas.AssignUserRole):
    """Назначение сборщика или курьера для заказа"""
    current_user = await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id == idx,
        docs_sales.c.is_deleted.is_not(True),
        docs_sales.c.cashbox == current_user.cashbox_id,
    )
    order = await database.fetch_one(query)

    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")

    if role == schemas.AssignUserRole.picker:
        update_field = "assigned_picker"
    else:  # courier
        update_field = "assigned_courier"

    query = (
        docs_sales.update()
        .where(docs_sales.c.id == idx)
        .values({update_field: current_user.id})
    )
    await database.execute(query)

    query = docs_sales.select().where(docs_sales.c.id == idx)
    updated_order = await database.fetch_one(query)
    updated_order = datetime_to_timestamp(updated_order)
    updated_order = await raschet_oplat(updated_order)
    updated_order = await add_docs_sales_settings(updated_order)

    query = docs_sales_goods.select().where(docs_sales_goods.c.docs_sales_id == idx)
    goods_db = await database.fetch_all(query)
    goods_db = [*map(datetime_to_timestamp, goods_db)]
    goods_db = [*map(add_nomenclature_name_to_goods, goods_db)]
    goods_db = [await instance for instance in goods_db]

    updated_order["goods"] = goods_db
    updated_order = await add_delivery_info_to_doc(updated_order)

    await manager.send_message(
        token,
        {
            "action": "assign_user",
            "target": "docs_sales",
            "id": idx,
            "role": role,
            "user_id": current_user.id,
        },
    )

    return updated_order


@router.get("/docs_sales/verify/{hash}")
async def verify_hash_and_get_order(hash: str, order_id: int, role: str):
    """Проверка валидности хеш-ссылки и получение информации о заказе"""
    expected_hash = generate_notification_hash(order_id, role)

    if hash != expected_hash:
        raise HTTPException(status_code=403, detail="Недействительная ссылка")

    if role == "general" or role == "courier":
        query = docs_sales.select().where(
            docs_sales.c.id == order_id, docs_sales.c.is_deleted.is_not(True)
        )
        order = await database.fetch_one(query)

        if not order:
            raise HTTPException(status_code=404, detail="Заказ не найден")

        if role == "general":
            order_data = datetime_to_timestamp(order)
            return order_data
        
        elif role == "courier":
            courier_data = {
                "id": order.id,
                "number": order.number,
                "status": order.order_status,
                "assigned_courier": order.assigned_courier,
            }

            query = docs_sales_delivery_info.select().where(
                docs_sales_delivery_info.c.docs_sales_id == order_id
            )
            delivery = await database.fetch_one(query)

            if delivery:
                courier_data["delivery"] = {
                    "address": delivery.address,
                    "delivery_date": delivery.delivery_date,
                    "delivery_price": delivery.delivery_price,
                    "recipient": delivery.recipient,
                    "note": delivery.note,
                }

            return courier_data

    elif role == "picker":
        query = f"""
            SELECT 
                sales.*,
                {', '.join(f'warehouse.{c.name} AS warehouse_{c.name}' for c in warehouses.c)},
                {', '.join(f'contragent.{c.name} AS contragent_{c.name}' for c in contragents.c)}
            FROM docs_sales sales
            LEFT JOIN warehouses warehouse ON warehouse.id = sales.warehouse
            LEFT JOIN contragents contragent ON contragent.id = sales.contragent
            WHERE sales.id = :order_id AND sales.is_deleted IS NOT TRUE
        """
        order = await database.fetch_one(query, { "order_id": order_id })
        order_dict = dict(order)

        if not order:
            raise HTTPException(status_code=404, detail="Заказ не найден")
        
        order_dict["status"] = order_dict["order_status"]

        query = f"""
            select
                "goods".*,
                {', '.join(f'nomenclature.{c.name} AS nomenclature_{c.name}' for c in nomenclature.c)},
                "pictures"."id" AS "picture_id",
                "pictures"."url" AS "picture_url",
                "pictures"."is_main" AS "picture_is_main",
                "unit"."id" as "nomenclature_unit_id",
                "unit"."convent_national_view" as "nomenclature_unit_convent_national_view"
            from "docs_sales_goods" "goods"
            left join "nomenclature" "nomenclature"
            on "goods"."nomenclature" = "nomenclature"."id"
            left join "units" "unit"
            on "nomenclature"."unit" = "unit"."id"
            left join lateral (
                select "id", "url", "is_main"
                from "pictures"
                where 
                    "entity" = 'nomenclature' AND 
                    "entity_id" = "nomenclature"."id"
                order by 
                    "is_main" desc,
                    "id" asc
                limit 1
            ) "pictures" on true
            where "goods"."docs_sales_id" = :order_id
        """
        goods = await database.fetch_all(query, { "order_id": order_id })

        if goods:
            order_dict["goods"] = goods

        # собираем инфу о доставке
        query = docs_sales_delivery_info.select().where(
            docs_sales_delivery_info.c.docs_sales_id == order_id
        )
        delivery = await database.fetch_one(query)

        if delivery:
            order_dict["delivery"] = {
                "address": delivery.address,
                "delivery_date": delivery.delivery_date,
                "delivery_price": delivery.delivery_price,
                "recipient": delivery.recipient,
                "note": delivery.note,
            }

        return order_dict
    else:
        raise HTTPException(status_code=400, detail="Неизвестная роль")