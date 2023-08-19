from fastapi import APIRouter, HTTPException, Depends

from const import PaymentType
from database.db import database, users_cboxes_relation, loyality_cards, users
import api.analytics.schemas as analytics_schemas
import functions.filter_schemas as filter_schemas
from functions.helpers import get_filters_analytics
from datetime import date, timedelta, datetime
from sqlalchemy import and_, func, select, distinct
from typing import Optional
import uuid

router = APIRouter(tags=["analytics"])


@router.get("/analytics/", response_model=analytics_schemas.PaymentsAnalytics)
async def analytics(
    token: str,
    entity: str = "payments",
    filter_schema: filter_schemas.AnalyticsFiltersQuery = Depends(),
    offset: int = 0,
    limit: int = 100,
    sort: str = "percentage:desc",
    type: str = f"{PaymentType.incoming}, {PaymentType.outgoing}",
):
    """Аналитика платежей"""
    query_user = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query_user)
    if user:
        if user.status:
            filters = get_filters_analytics(filter_schema)
            payment_directions = type.split(", ")
            cashbox = user.cashbox_id
            queries = []
            for payment_direction in payment_directions:
                query_sums = "("+f"""
                    SELECT articles.name AS name, payments.type AS type, sum(payments.amount) AS sum
                    FROM payments JOIN articles ON articles.id = payments.article_id
                    WHERE payments.cashbox = {cashbox} AND payments.type = '{payment_direction}' AND payments.is_deleted != true
                    """ + filters + " GROUP BY articles.name, payments.type"+") as sums"
                query_articles = "(" + f"""
                    SELECT articles.id AS id, articles.name AS name, articles.emoji AS emoji, articles.icon_file AS icon_file 
                    FROM articles
                    WHERE articles.cashbox = {cashbox}
                """+") as articles"
                total_column = f"""
                    SELECT sum(payments.amount) AS total
                    FROM payments JOIN articles ON articles.id = payments.article_id
                    WHERE payments.cashbox = {cashbox} AND payments.type = '{payment_direction}' AND payments.is_deleted != true
                """ + filters
                queries_joined = query_articles + " JOIN " + query_sums + " ON sums.name=articles.name"

                queries.append(
                    f"""
                    SELECT articles.id, articles.icon_file, articles.emoji, articles.name, sums.type, sums.sum,
                        sums.sum * 100 / ({total_column}) as percentage
                    FROM {queries_joined}
                    """
                )
            query = queries.pop(0)
            for subquery in queries:
                query = query + " UNION " + subquery

            sort_name, sort_direction = sort.split(":")[:2]
            if sort_name not in ("percentage", ):
                raise HTTPException(
                    status_code=400, detail="Вы ввели некорректный параметр сортировки!"
                )
            if sort_direction in ("desc", "asc"):
                query = query + f"ORDER BY {sort_name} {sort_direction}"
            else:
                raise HTTPException(
                    status_code=400, detail="Вы ввели некорректный параметр сортировки!"
                )
            query = query + f" LIMIT {limit} OFFSET {offset}"
            articles_db = await database.fetch_all(query)

            result = map(
                lambda row: analytics_schemas.PaymentAnalytics(
                    article_id=row[0],
                    article_image=row[1],
                    article_emoji=row[2],
                    article_name=row[3],
                    type=row[4],
                    sum=row[5],
                    percentage=row[6],
                ),
                articles_db,
            )
            return [*result]
    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")




@router.get("/analytics_cards/")
async def analytics(
    token: str,
    date_from: int,
    date_to: int,
    user_id: Optional[int] = None,
):
    
    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    """Аналитика карт лояльности"""
    query_user = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query_user)
    if user:
        if user.status:
            start_date = datetime.fromtimestamp(date_from)
            end_date = datetime.fromtimestamp(date_to)

            res = []
            
            filters = [users_cboxes_relation.c.cashbox_id == user.cashbox_id]
            if user_id:
                filters.append(users_cboxes_relation.c.user == user_id)

            all_distinct_users_q = users_cboxes_relation.select().where(*filters)
            all_distinct_users = await database.fetch_all(all_distinct_users_q)

            for c_user in all_distinct_users:
                user_tg_q = users.select().where(users.c.id == c_user.user)
                user_tg = await database.fetch_one(user_tg_q)

                q = select(func.count(loyality_cards.c.id)).where(loyality_cards.c.cashbox_id == user.cashbox_id, loyality_cards.c.created_by_id == c_user.id)
                all_cards = await database.fetch_one(q)

                user_body = {
                    "username": user_tg.username,
                    "user_id": user_tg.id,
                    "first_name": user_tg.first_name,
                    "all_count": all_cards.count_1,
                }
                subres = []

                for single_date in daterange(start_date, end_date):
                    day_start = single_date.replace(hour=0, minute=0, second=0, microsecond=000000)
                    day_end = single_date.replace(hour=23, minute=59, second=59, microsecond=999999)

                    q = loyality_cards.select().where(loyality_cards.c.cashbox_id == user.cashbox_id, loyality_cards.c.created_at >= day_start, loyality_cards.c.created_at <= day_end, loyality_cards.c.created_by_id == c_user.id)
                    all_cards = await database.fetch_all(q)

                    subres_body = {
                        "id": uuid.uuid4(),
                        "date": single_date.strftime("%d-%m-%Y"),
                        "day_count": len(all_cards),
                    }

                    subres.append(subres_body)

                
                user_body['result'] = subres
                res.append(user_body)
            return res


    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")