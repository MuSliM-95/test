from fastapi import APIRouter
from sqlalchemy import select, func, desc, case, and_
from database.db import database, payments, pboxes
from . import schemas
from functions.helpers import get_user_by_token


router = APIRouter(tags=["reports"])


@router.get("/sales/")
async def get_sales_report(token: str):
    user = await get_user_by_token(token)
    pass


@router.get("/balances/")
async def get_balances_report(token: str, paybox: int, datefrom: int, dateto: int, user: int = None):
    await get_user_by_token(token)

    filters = [
        payments.c.paybox == paybox,
        and_(datefrom <= payments.c.date, payments.c.date <= dateto),
        payments.c.is_deleted.is_not(True)
    ]

    if user:
        filters.append(payments.c.account)


    query_incoming = select(
        payments.c.paybox,
        pboxes.c.name,
        payments.c.type,
        func.sum(payments.c.amount).label('incoming'),
    ).\
        where(*filters, payments.c.type == 'incoming').\
        join(pboxes, pboxes.c.id == payments.c.paybox).\
        group_by(payments.c.paybox, payments.c.type, pboxes.c.name).subquery('query_incoming')

    query_outgoing = select(
        payments.c.paybox,
        pboxes.c.name,
        payments.c.type,
        func.sum(payments.c.amount).label('outgoing'),
    ).\
        where(*filters, payments.c.type == 'outgoing').\
        join(pboxes, pboxes.c.id == payments.c.paybox).\
        group_by(payments.c.paybox, payments.c.type, pboxes.c.name).subquery('query_outgoing')

    query = select(pboxes.c.name, query_incoming.c.incoming, query_outgoing.c.outgoing).where(pboxes.c.id == paybox)

    report = await database.fetch_all(query)
    return report