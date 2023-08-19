import random
import string
import hashlib
from datetime import datetime
from typing import Optional

import pytz
from databases.backends.postgres import Record
from fastapi import HTTPException
from sqlalchemy import Table, cast, String, and_

from const import PaymentType
from database.db import (
    users_cboxes_relation,
    database,
    entity_to_entity,
    nomenclature,
    contragents,
    payments,
    docs_sales_goods,
    loyality_transactions,
    organizations,
    units,
    entity_or_function,
    fifo_settings,
)


def gen_token():
    letters = string.ascii_letters
    rand_string = "".join(random.choice(letters) for i in range(32))
    token = hashlib.sha256(rand_string.encode("utf-8")).hexdigest()
    return token


def get_filters(table, filters):
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "name":
            if value:
                filters_list.append((f"payments.name ILIKE '%{value}%'"))
        elif filter == "tags":
            if value:
                filters_list.append((f"payments.tags ILIKE '%{value}%'"))
        elif filter == "project":
            if value:
                filters_list.append(
                    (
                        f"payments.project_id IN (SELECT projects.id FROM projects WHERE projects.name ILIKE '%{value}%' AND projects.cashbox = payments.cashbox)"
                    )
                )
        elif filter == "contragent":
            if value:
                filters_list.append(
                    (
                        f"payments.contragent IN (SELECT contragents.id FROM contragents WHERE contragents.name ILIKE '%{value}%' AND contragents.cashbox = payments.cashbox)"
                    )
                )
        elif filter == "paybox":
            if value:
                filters_list.append(
                    (
                        f"payments.paybox IN (SELECT payboxes.id FROM payboxes WHERE payboxes.name ILIKE '%{value}%' AND payboxes.cashbox = payments.cashbox)"
                    )
                )
        elif filter == "paybox_to":
            if value:
                filters_list.append(
                    (
                        f"payments.paybox_to IN (SELECT payboxes.id FROM payboxes WHERE payboxes.name ILIKE '%{value}%' AND payboxes.cashbox = payments.cashbox)"
                    )
                )
        elif filter == "payment_type":
            if value:
                if value in (
                    PaymentType.incoming,
                    PaymentType.outgoing,
                    PaymentType.transfer,
                ):
                    filters_list.append((f"payments.type = '{value}'"))
        elif filter == "external_id":
            if value:
                filters_list.append((f"payments.external_id ILIKE '%{value}%'"))
        elif filter == "relship":
            if value:
                if value == "parents":
                    # filters_list.append(table.c.parent_id == None)
                    filters_list.append((f"payments.parent_id IS NULL"))
                elif value == "childs":
                    # filters_list.append(table.c.parent_id != None)
                    filters_list.append((f"payments.parent_id IS NOT NULL"))

    datefrom = None
    dateto = None

    if filters_dict["datefrom"]:
        datefrom = pytz.utc.localize(
            datetime.strptime(filters_dict["datefrom"], "%d-%m-%Y")
        ).timestamp()

    if filters_dict["dateto"]:
        dateto = pytz.utc.localize(
            datetime.strptime(filters_dict["dateto"], "%d-%m-%Y")
        ).timestamp()

    if datefrom and not dateto:
        # filters_list.append(table.c.date >= datefrom)
        filters_list.append((f"payments.date >= {int(datefrom)}"))

    if not datefrom and dateto:
        # filters_list.append(table.c.date <= dateto)
        filters_list.append((f"payments.date <= {int(dateto)}"))

    if datefrom and dateto:
        filters_list.append(
            (f"payments.date >= {int(datefrom)} AND payments.date <= {int(dateto)}")
        )

    if filters_list:
        return f"AND {' AND '.join(filters_list)}"
    else:
        return ""


def get_filters_analytics(filters) -> str:
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "paybox_id":
            if value:
                values: list[str] = value.split(",")
                expression = ""
                for val in values:
                    if val.isdigit():
                        expression = expression + val + ", "
                    else:
                        raise ValueError
                expression = expression.rstrip(", ")
                filters_list.append(
                    f"""
                    AND payments.paybox IN
                    (SELECT payboxes.id FROM payboxes
                    WHERE payboxes.id IN ({expression}) AND payboxes.cashbox = payments.cashbox)
                """
                )
        elif filter == "status":
            if value:
                values: list[str] = value.split(",")
                expression = ""
                for val in values:
                    if val in ("true", "false"):
                        expression = expression + val + ", "
                    else:
                        raise ValueError
                expression = expression.rstrip(", ")
                filters_list.append(f" AND payments.status IN ({expression})")

    datefrom = filters_dict["datefrom"]
    dateto = filters_dict["dateto"]

    if datefrom:
        filters_list.append(f" AND payments.date >= {int(datefrom)}")

    if dateto:
        filters_list.append(f" AND payments.date <= {int(dateto)}")

    return " ".join(filters_list)



def get_filters_transactions(table, filters):

    filters_list = []
    filters_dict = filters.dict()

    def _periods_filter(period_type):
        datefrom = filters_dict.get(f"{period_type}_from")
        dateto = filters_dict.get(f"{period_type}_to")

        if datefrom and not dateto:
            filters_list.append(table.c.dated >= datetime.fromtimestamp(datefrom))

        if not datefrom and dateto:
            filters_list.append(table.c.dated <= datetime.fromtimestamp(dateto))

        if datefrom and dateto:
            filters_list.append(and_(table.c.dated >= datetime.fromtimestamp(datefrom), table.c.dated <= datetime.fromtimestamp(dateto)))
    
    _periods_filter("dated")

    for filter, value in filters_dict.items():
        if filter == "type":
            if value:
                filters_list.append(table.c.type == value)
        if filter == "loyality_card_number":
            if value:
                filters_list.append(cast(table.c.loyality_card_number, String).ilike(f"%{value}%"))
        if filter == "amount":
            if value:
                filters_list.append(table.c.amount == value)
        if filter == "tags":
            if value:
                filters_list.append(table.c.tags.ilike(f"%{value}%"))
        if filter == "name":
            if value:
                filters_list.append(table.c.name.ilike(f"%{value}%"))
        if filter == "description":
            if value:
                filters_list.append(table.c.description.ilike(f"%{value}%"))

    return filters_list



def get_filters_cards(table, filters):

    filters_list = []
    filters_dict = filters.dict()

    def _periods_filter(period_type):
        datefrom = filters_dict.get(f"{period_type}_from")
        dateto = filters_dict.get(f"{period_type}_to")

        if datefrom and not dateto:
            if period_type == "start_period":
                filters_list.append(table.c.start_period >= datetime.fromtimestamp(datefrom))
            else:
                filters_list.append(table.c.end_period >= datetime.fromtimestamp(datefrom))

        if not datefrom and dateto:
            if period_type == "start_period":
                filters_list.append(table.c.start_period <= datetime.fromtimestamp(dateto))
            else:
                filters_list.append(table.c.end_period <= datetime.fromtimestamp(dateto))

        if datefrom and dateto:
            if period_type == "start_period":
                filters_list.append(and_(table.c.start_period >= datetime.fromtimestamp(datefrom), table.c.start_period <= datetime.fromtimestamp(dateto)))
            else:
                filters_list.append(and_(table.c.end_period >= datetime.fromtimestamp(datefrom), table.c.end_period <= datetime.fromtimestamp(dateto)))
    
    _periods_filter("start_period")
    _periods_filter("end_period")

    for filter, value in filters_dict.items():
        if filter == "card_number":
            if value:
                filters_list.append(cast(table.c.card_number, String).ilike(f"%{value}%"))
        if filter == "balance":
            if value:
                filters_list.append(table.c.balance == value)
        if filter == "income":
            if value:
                filters_list.append(table.c.income == value)
        if filter == "outcome":
            if value:
                filters_list.append(table.c.outcome == value)
        if filter == "cashback_percent":
            if value:
                filters_list.append(table.c.cashback_percent == value)
        if filter == "minimal_checque_amount":
            if value:
                filters_list.append(table.c.minimal_checque_amount == value)
        if filter == "max_percentage":
            if value:
                filters_list.append(table.c.max_percentage == value)
        if filter == "status_card":
            if value:
                filters_list.append(table.c.status_card == value)
        if filter == "created_at_from":
            if value:
                filters_list.append(table.c.created_at >= datetime.fromtimestamp(value))
        if filter == "created_at_to":
            if value:
                filters_list.append(table.c.created_at <= datetime.fromtimestamp(value))

    return filters_list


def get_filters_pboxes(table, filters):
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "external_id":
            if value:
                filters_list.append(table.c.external_id.ilike(f"%{value}%"))
        if filter == "name":
            if value:
                filters_list.append(table.c.name.ilike(f"%{value}%"))

    return filters_list


def get_filters_projects(table, filters):
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "external_id":
            if value:
                filters_list.append(table.c.external_id.ilike(f"%{value}%"))
        if filter == "name":
            if value:
                filters_list.append(table.c.name.ilike(f"%{value}%"))

    return filters_list


def get_filters_articles(table, filters):
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "name":
            if value:
                filters_list.append(table.c.name.ilike(f"%{value}%"))
    return filters_list


def get_filters_users(table, filters):
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "external_id":
            if value:
                filters_list.append(table.c.external_id.ilike(f"%{value}%"))

    return filters_list


def get_filters_ca(table, filters):
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "name":
            if value:
                filters_list.append(table.c.name.ilike(f"%{value}%"))
        elif filter == "inn":
            if value:
                filters_list.append(table.c.inn.ilike(r"%{}%".format(value)))
        elif filter == "phone":
            if value:
                filters_list.append(table.c.phone.ilike(r"%{}%".format(value)))
        elif filter == "external_id":
            if value:
                filters_list.append(table.c.external_id.ilike(r"%{}%".format(value)))

    return filters_list


def get_filters_cheques(table, filters):
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "user":
            if value:
                filters_list.append(table.c.user == value)

        date_from = filters_dict["datefrom"]
        date_to = filters_dict["dateto"]

        if date_from:
            filters_list.append(table.c.created_at >= int(date_from))

        if date_to:
            filters_list.append(table.c.created_at <= int(date_to))

    return filters_list


def raise_wrong_token():
    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


async def add_nomenclature_count(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)

        q = docs_sales_goods.select().where(docs_sales_goods.c.docs_sales_id == instance['id'])
        goods = await database.fetch_all(q)

        if goods:
            instance["nomenclature_count"] = len(goods)
            instance["doc_discount"] = round(sum([good.sum_discounted for good in goods]), 2)
        else:
            instance["nomenclature_count"] = 0
            instance["doc_discount"] = 0
        
        return instance
    

async def add_nomenclature_name_to_goods(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)

        q = nomenclature.select().where(nomenclature.c.id == instance['nomenclature'])
        nomenclature_db = await database.fetch_one(q)

        if instance.get("unit"):
            q = units.select().where(units.c.id == instance.get("unit"))
            unit_db = await database.fetch_one(q)

            if unit_db:
                instance['unit_name'] = unit_db.convent_national_view

        if nomenclature_db:
            instance["nomenclature_name"] = nomenclature_db.name
        else:
            instance["nomenclature_name"] = ""
        
        return instance


async def raschet_oplat(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)

        proxyes_q = entity_to_entity.select().where(entity_to_entity.c.cashbox_id == instance['cashbox'], entity_to_entity.c.from_id == instance['id'])
        proxyes = await database.fetch_all(proxyes_q)

        paid_rubles = 0
        paid_loyality = 0

        for proxy in proxyes:
            print(proxy)
            if proxy.from_entity == 7:

                # Платеж

                if proxy.to_entity == 5:
                    q_payment = payments.select().where(
                        payments.c.id == proxy.to_id,
                        payments.c.cashbox == instance['cashbox'],
                        payments.c.status == True, 
                        payments.c.is_deleted == False
                    )
                    payment = await database.fetch_one(q_payment)
                    if payment:
                        paid_rubles += payment.amount

                # Транзакция
                if proxy.to_entity == 6:
                    q_trans = loyality_transactions.select().where(
                        loyality_transactions.c.id == proxy.to_id,
                        loyality_transactions.c.cashbox == instance['cashbox'],
                        loyality_transactions.c.status == True, 
                        loyality_transactions.c.is_deleted == False
                    )
                    trans = await database.fetch_one(q_trans)
                    if trans:
                        paid_loyality += trans.amount


        paid_rubles = round(paid_rubles, 2)
        paid_loyality = round(paid_loyality, 2)

        instance["paid_rubles"] = paid_rubles
        instance["paid_loyality"] = paid_loyality
        instance["paid_doc"] = paid_loyality + paid_rubles
        
        return instance

async def nomenclature_unit_id_to_name(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)
        
        q = units.select().where(units.c.id == instance.get("unit"))
        unit_db = await database.fetch_one(q)

        if unit_db:
            instance['unit_name'] = unit_db.convent_national_view

        return instance

def datetime_to_timestamp(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)
        if instance.get("start_period"):
            instance["start_period"] = int(instance["start_period"].timestamp())
        if instance.get("end_period"):
            instance["end_period"] = int(instance["end_period"].timestamp())

        if instance.get("dead_at"):
            instance["dead_at"] = int(instance["dead_at"].timestamp())
        if instance.get("dated"):
            try:
                instance["dated"] = int(instance["dated"].timestamp())
            except AttributeError:
                pass

        if instance.get("created_at"):
            instance["created_at"] = int(instance["created_at"].timestamp())
        if instance.get("updated_at"):
            instance["updated_at"] = int(instance["updated_at"].timestamp())
        return instance
    

def add_status(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)
        instance["data"] = {
            "status": "success"
        }
    
        return instance


async def get_user_by_token(token: str) -> Record:
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if not user or not user.status:
        raise_wrong_token()
    return user


async def get_entity_by_id(entity: Table, idx: int, owner: int) -> Record:
    """Returns entity from db, filtered by owner and is_deleted fields"""

    query = entity.select().where(
        entity.c.id == idx,
        entity.c.owner == owner,
        entity.c.is_deleted.is_not(True),
    )
    entity_db = await database.fetch_one(query)

    if not entity_db:
        raise HTTPException(
            status_code=404, detail=f"У вас нет {entity.name.rstrip('s')} с таким id."
        )

    return entity_db

async def get_entity_by_id_and_created_by(entity: Table, idx: int, created_by: int) -> Record:
    """Returns entity from db, filtered by owner and is_deleted fields"""

    query = entity.select().where(
        entity.c.id == idx,
        entity.c.created_by_id == created_by,
        entity.c.is_deleted.is_not(True),
    )
    entity_db = await database.fetch_one(query)

    if not entity_db:
        raise HTTPException(
            status_code=404, detail=f"У вас нет {entity.name.rstrip('s')} с таким id."
        )

    return entity_db

async def get_entity_by_id_cashbox(entity: Table, idx: int, cashbox_id: int) -> Record:
    """Returns entity from db, filtered by cashbox_id and is_deleted fields"""

    query = entity.select().where(
        entity.c.id == idx,
        entity.c.cashbox == cashbox_id,
        entity.c.is_deleted.is_not(True),
    )
    entity_db = await database.fetch_one(query)

    if not entity_db:
        raise HTTPException(
            status_code=404, detail=f"У вас нет {entity.name.rstrip('s')} с таким id."
        )

    return entity_db


async def contr_org_ids_to_name(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)

        contr_id = instance['contragent_id']
        org_id = instance['organization_id']

        query = contragents.select().where(contragents.c.id == contr_id)
        contr = await database.fetch_one(query)

        query = organizations.select().where(organizations.c.id == org_id)
        org = await database.fetch_one(query)

        instance['contragent'] = contr.name
        instance['organization'] = org.short_name

        return instance


async def check_contragent_exists(idx, cashbox_id):
    query = contragents.select().where(
        contragents.c.id == idx, contragents.c.cashbox == cashbox_id
    )
    if not await database.fetch_one(query):
        raise HTTPException(
            status_code=403,
            detail="Введенный контрагент не принадлежит вам или не существует!",
        )
    return True


async def check_unit_exists(unit_id):
    query = units.select().where(units.c.id == unit_id)
    if not await database.fetch_one(query):
        raise HTTPException(
            status_code=403,
            detail="Единицы измерения с этим id не существует!",
        )
    return True


async def check_function_exists(name: str):
    query = entity_or_function.select().where(entity_or_function.c.name == name)
    if not await database.fetch_one(query):
        raise HTTPException(
            status_code=404,
            detail=f"Функция не существует!",
        )
    return True


async def check_entity_exists(entity: Table, idx, user_id=None):
    query = entity.select().where(entity.c.id == idx)
    if not await database.fetch_one(query):
        raise HTTPException(
            status_code=404,
            detail=f"{entity.name.rstrip('s')} не существует!",
        )
    return True


async def check_period_blocked(organization_id: int, date: int, exceptions: list[str]):
    if organization_id is not None and date:
        query = fifo_settings.select().where(
            fifo_settings.c.organization_id == organization_id,
            fifo_settings.c.blocked_date >= date,
        )
        if await database.fetch_one(query):
            exceptions.append(
                f"Период закрыт для организации {organization_id} на указанную дату."
            )
            return False
    return True
