import os

from sqlalchemy import select, func, and_, desc, case

from database.db import (
    database,
    warehouses,
    warehouse_balances,
    nomenclature,
    marketplace_rating_aggregates,
    docs_sales,
    cboxes,
    users
)

from .schemas import SellerStatisticsItem, SellerStatisticsResponse

class MarketplaceSellerStatisticsService:

    @staticmethod
    def __transform_photo_route(photo_path: str) -> str:
        base_url = os.getenv("APP_URL")
        photo_url = photo_path.lstrip("/")

        if "seller" in photo_url:
            return f'https://{base_url}/api/v1/{photo_path.lstrip("/")}'
        else:
            return f'https://{base_url}/{photo_path.lstrip("/")}'


    async def get_sellers_statistics(self) -> SellerStatisticsResponse:
        # 1. Получаем всех актуальных селлеров (balance > 0)
        sellers_rows = await database.fetch_all(
            select(
                cboxes.c.id,

                func.coalesce(
                    func.nullif(cboxes.c.seller_name, ""),
                    cboxes.c.name,
                ).label("seller_name"),

                cboxes.c.seller_description,

                func.coalesce(
                    func.nullif(cboxes.c.seller_photo, ""),
                    users.c.photo,
                ).label("seller_photo"),

                cboxes.c.created_at
            ).where(cboxes.c.balance > 0)
        )

        sellers = []

        for row in sellers_rows:
            seller_id = row["id"]
            # 2. Кол-во активных складов
            active_warehouses_row = await database.fetch_one(
                select(func.count(warehouses.c.id)).where(
                    and_(
                        warehouses.c.cashbox == seller_id,
                        warehouses.c.status.is_(True),
                        warehouses.c.is_deleted.is_not(True)
                    )
                )
            )
            active_warehouses = active_warehouses_row[0] or 0
            # 3. Кол-во товаров на складах селлера
            total_products_row = await database.fetch_one(
                select(func.sum(warehouse_balances.c.current_amount))
                .select_from(warehouse_balances)
                .join(
                    nomenclature,
                    nomenclature.c.id == warehouse_balances.c.nomenclature_id
                )
                .where(nomenclature.c.cashbox == seller_id)
            )
            total_products = total_products_row[0] or 0
            # 4. Рейтинг селлера
            rating_row = await database.fetch_one(
                select(
                    func.avg(marketplace_rating_aggregates.c.avg_rating).label("rating"),
                    func.sum(marketplace_rating_aggregates.c.reviews_count).label("reviews")
                )
                .select_from(marketplace_rating_aggregates)
                .join(
                    nomenclature,
                    nomenclature.c.id == marketplace_rating_aggregates.c.entity_id
                )
                .where(
                    and_(
                        marketplace_rating_aggregates.c.entity_type == "nomenclature",
                        nomenclature.c.cashbox == seller_id
                    )
                )
            )

            rating = float(rating_row["rating"]) if rating_row["rating"] is not None else None
            reviews_count = int(rating_row["reviews"]) if rating_row["reviews"] is not None else 0
            # 5. Заказы селлера
            orders_row = await database.fetch_one(
                select(
                    func.count(docs_sales.c.id).label("total"),
                    func.sum(
                        case(
                            (docs_sales.c.order_status.in_(["delivered", "success"]), 1),
                            else_=0
                        )
                    ).label("completed"),
                    func.max(docs_sales.c.created_at).label("last_order")
                )
                .where(docs_sales.c.cashbox == seller_id)
            )

            orders_total = orders_row["total"] or 0
            orders_completed = orders_row["completed"] or 0
            last_order_date = orders_row["last_order"]

            if row["seller_photo"]:
                row["seller_photo"] = self.__transform_photo_route(row["seller_photo"])

            sellers.append({
                "id": seller_id,
                "seller_name": row["seller_name"],
                "seller_description": row["seller_description"],
                "seller_photo": row["seller_photo"],

                "rating": rating,
                "reviews_count": reviews_count,

                "orders_total": orders_total,
                "orders_completed": orders_completed,
                "registration_date": row["created_at"],
                "last_order_date": last_order_date,

                "active_warehouses": active_warehouses,
                "total_products": total_products
            })

        return SellerStatisticsResponse(
            sellers=[SellerStatisticsItem(**s) for s in sellers]
        )