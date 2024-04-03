from fastapi import APIRouter
from sqlalchemy import select, func, desc, case, and_, text
from database.db import database, payments, pboxes, docs_sales, docs_sales_goods, nomenclature
from . import schemas
from functions.helpers import get_user_by_token


router = APIRouter(tags=["reports"])


@router.post("/reports/sales/")
async def get_sales_report(token: str, report_data: schemas.ReportData):
    user = await get_user_by_token(token)

    sales = (
            select(nomenclature.c.id,
                   payments.c.paybox,
                   nomenclature.c.name,
                   func.sum(docs_sales_goods.c.quantity).label('count'),
                   func.sum(docs_sales_goods.c.quantity)*docs_sales_goods.c.price
                   ).
            where(payments.c.paybox == 1).
            join(payments, payments.c.docs_sales_id == docs_sales_goods.c.docs_sales_id).
            join(nomenclature, nomenclature.c.id == docs_sales_goods.c.nomenclature).
            join(docs_sales, docs_sales.c.id == docs_sales_goods.c.docs_sales_id).
            group_by(nomenclature.c.id,payments.c.paybox,nomenclature.c.name)
             )
    print(sales)
    return {}


@router.post("/reports/balances/")
async def get_balances_report(token: str, report_data: schemas.ReportData):
    await get_user_by_token(token)
    report = []
    for paybox in report_data.paybox:
        filters = [
            payments.c.paybox == paybox,
            text(f'payments.date >= {report_data.datefrom}'),
            text(f'payments.date <= {report_data.dateto}'),
            payments.c.is_deleted.is_not(True)
        ]

        if report_data.user:
            filters.append(payments.c.account == report_data.user)

        query_incoming = select(
            payments.c.paybox,
            pboxes.c.name,
            payments.c.type,
            func.sum(payments.c.amount).label('incoming'),
        ).\
            where(*filters,
                  payments.c.type == 'incoming',
                  ).\
            join(pboxes, pboxes.c.id == payments.c.paybox).\
            group_by(payments.c.paybox, payments.c.type, pboxes.c.name)

        query_outgoing = select(
            payments.c.paybox,
            pboxes.c.name,
            payments.c.type,
            func.sum(payments.c.amount).label('outgoing'),
        ).\
            where(*filters, payments.c.type == 'outgoing',
                  ).\
            join(pboxes, pboxes.c.id == payments.c.paybox).\
            group_by(payments.c.paybox, payments.c.type, pboxes.c.name)

        report_db_in = [dict(item) for item in await database.fetch_all(query_incoming)]
        report_db_out = [dict(item) for item in await database.fetch_all(query_outgoing)]

        query = select(pboxes.c.name, pboxes.c.balance).where(pboxes.c.id == paybox)
        report_db = dict(await database.fetch_one(query))
        report_db['incoming'] = report_db_in[0]['incoming'] if len(report_db_in) > 0 else 0
        report_db['outgoing'] = report_db_out[0]['outgoing'] if len(report_db_out) > 0 else 0

        if report_db:
            report.append(report_db)
    return report