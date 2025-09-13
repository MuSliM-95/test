from sqlalchemy import select, desc, func

from database.db import contragents, docs_sales, docs_sales_delivery_info


class DocsSalesListRepository:
    @staticmethod
    def base_list_query(user_cashbox_id: int):
        base_query = (
            select(*docs_sales.columns, contragents.c.name.label("contragent_name"))
            .select_from(docs_sales)
            .outerjoin(contragents, docs_sales.c.contragent == contragents.c.id)
            .where(
                docs_sales.c.is_deleted.is_not(True),
                docs_sales.c.cashbox == user_cashbox_id,
            )
            .order_by(desc(docs_sales.c.id))
        )
        return base_query

    @staticmethod
    def base_count_list_query(user_cashbox_id: int):
        count_query = (
            select(func.count())
            .select_from(docs_sales)
            .where(
                docs_sales.c.is_deleted.is_not(True),
                docs_sales.c.cashbox == user_cashbox_id,
            )
        )
        return count_query


    def query_by_created_date(self, user_cashbox_id: int, start: int, end: int):
        return self.base_list_query(user_cashbox_id) \
               .where(docs_sales.c.dated >= start, docs_sales.c.dated <= end)


    def query_by_delivery_date(self, user_cashbox_id: int, start, end):
        return self.base_list_query(user_cashbox_id) \
               .outerjoin(docs_sales_delivery_info,
                          docs_sales_delivery_info.c.docs_sales_id == docs_sales.c.id) \
               .where(docs_sales_delivery_info.c.delivery_date >= start,
                      docs_sales_delivery_info.c.delivery_date <= end)
