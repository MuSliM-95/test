import json

from sqlalchemy import select, and_

from database.db import docs_sales, OrderStatus, database, segments, users, users_cboxes_relation, docs_sales_tags

from segments.ranges import apply_date_range, apply_range
from sqlalchemy.sql import Select

from segments.base_logic import BaseSegmentLogic

from segments.segment_tg_notification import send_segment_notification

from api.docs_sales.routers import generate_and_save_order_links

from segments.masks import replace_masks


class DocsSalesLogic(BaseSegmentLogic):

    @staticmethod
    def add_picker_filters(query: Select, picker_filters):

        where_clauses = []

        assigned = picker_filters.get("assigned")

        if assigned is not None:
            apply_range(docs_sales.c.assigned_picker,
                        {"is_none": not assigned}, where_clauses)


        if sdr := picker_filters.get("start"):
            apply_date_range(docs_sales.c.picker_started_at, sdr, where_clauses)
        if fdr := picker_filters.get("finish"):
            apply_date_range(docs_sales.c.picker_finished_at, fdr, where_clauses)

        if where_clauses:
            query = query.where(and_(*where_clauses))

        return query

    @staticmethod
    def add_courier_filters(query: Select, courier_filters):

        where_clauses = []

        assigned = courier_filters.get("assigned")

        if assigned is not None:
            apply_range(docs_sales.c.assigned_courier,
                        {"is_none": not assigned}, where_clauses)

            if assigned is False:
                apply_range(docs_sales.c.order_status,
                            {"eq": OrderStatus.collected.value}, where_clauses)

        if sdr := courier_filters.get("start"):
            apply_date_range(docs_sales.c.courier_picked_at, sdr, where_clauses)
        if fdr := courier_filters.get("finish"):
            apply_date_range(docs_sales.c.courier_delivered_at, fdr, where_clauses)

        if where_clauses:
            query = query.where(and_(*where_clauses))

        return query

    async def update_segment(self):

        criteria_data = json.loads(self.segment_obj.criteria)

        query = (
            select(docs_sales.c.id)
            .distinct(docs_sales.c.id)
            .where(docs_sales.c.cashbox == self.segment_obj.cashbox_id)
        )
        if tag := criteria_data.get("tag"):
            query = (
                query
                .join(docs_sales_tags, docs_sales_tags.c.docs_sales_id == docs_sales.c.id)
                .where(docs_sales_tags.c.name == tag)
            )

        where_clauses = []  # фильтры

        if cr_at := criteria_data.get("created_at"):
            apply_date_range(docs_sales.c.created_at, cr_at, where_clauses)

        if criteria_data.get("picker"):
            query = self.add_picker_filters(query, criteria_data.get("picker"))

        if criteria_data.get("courier"):
            query = self.add_courier_filters(query, criteria_data.get("courier"))

        if where_clauses:
            query = query.where(and_(*where_clauses))
        rows = await database.fetch_all(query)
        docs_sales_to_segment = [row.id for row in rows]
        docs_sales_in_segment = self.segment_obj.current_ids if self.segment_obj.current_ids is not None else []
        ids_to_segment = list(
            set(docs_sales_to_segment) - set(docs_sales_in_segment))
        ids_out_of_segment = list(
            set(docs_sales_in_segment) - set(docs_sales_to_segment))
        changes = {}
        changes["last_added_ids"] = ids_to_segment
        changes["last_removed_ids"] = ids_out_of_segment
        if ids_to_segment or ids_out_of_segment:
            changes["current_ids"] = docs_sales_to_segment
        if changes:
            await database.execute(
                segments.update().where(segments.c.id == self.segment_obj.id)
                .values(
                    **changes
                )
            )

        return docs_sales_to_segment

    async def collect_data(self):
        return {}

    def actions(self, action):
        act = {
            "send_tg_notification": self.send_tg_notification,
        }
        return act.get(action)

    async def start_actions(self):
        await self.refresh_segment_obj()
        actions = json.loads(self.segment_obj.actions)
        for k,v in actions.items():
            if self.actions(k) is None:
                continue
            await self.actions(k)(**v)
        return

    async def send_tg_notification(self, trigger_on_new: bool, message: str, user_tag: str = None, send_to: str = None):
        if not trigger_on_new or not user_tag:
            return  # Заглушка, пока нет решения как должны отрабатывать actions при каждом обновлении сегмента
        if send_to is None or send_to not in ["picker", "courier"]:
            chat_ids = await self.get_user_chat_ids_by_tag(user_tag)
        if trigger_on_new:
            if not self.segment_obj.last_added_ids:
                return
            for order_id in self.segment_obj.last_added_ids:
                message_text = f'Заказ # - {str(order_id)}\n\n' + message

                replacements = await self.link_replacements(order_id)

                message_text = replace_masks(message_text, replacements)
                if send_to == "picker":
                    chat_ids = await self.get_picker_chat_id(order_id)
                elif send_to == "courier":
                    chat_ids = await self.get_courier_chat_id(order_id)
                await send_segment_notification(
                    recipient_ids=chat_ids,
                    notification_text=message_text,
                    segment_id=self.segment_obj.id,
                )

    async def link_replacements(self, order_id):
        links = await generate_and_save_order_links(order_id)
        replacements = {}
        for k,v in links.items():
            replacements[k] = f"\n\n<a href='{v['url']}'>Открыть заказ</a>"
        return replacements

    async def get_user_chat_ids_by_tag(self, user_tag: str):
        query = (
            select(users.c.chat_id)
            .join(users_cboxes_relation,
                  users_cboxes_relation.c.user == users.c.id)
            .where(and_(
                users_cboxes_relation.c.cashbox_id == self.segment_obj.cashbox_id,
                users_cboxes_relation.c.tags.op('~')(
                    fr'(?<=^|,){user_tag}(?=,|$)')
            ))
        )
        rows = await database.fetch_all(query)
        return [row.chat_id for row in rows]

    async def get_picker_chat_id(self, order_id: int):
        query = (
            select(users.c.chat_id)
            .join(users_cboxes_relation, users_cboxes_relation.c.user == users.c.id)
            .outerjoin(docs_sales, docs_sales.c.assigned_picker == users_cboxes_relation.c.id)
            .where(and_(docs_sales.c.id == order_id, docs_sales.c.cashbox == self.segment_obj.cashbox_id))
        )
        rows = await database.fetch_all(query)
        return [row.chat_id for row in rows]

    async def get_courier_chat_id(self, order_id: int):
        query = (
            select(users.c.chat_id)
            .join(users_cboxes_relation, users_cboxes_relation.c.user == users.c.id)
            .outerjoin(docs_sales, docs_sales.c.assigned_courier == users_cboxes_relation.c.id)
            .where(and_(docs_sales.c.id == order_id, docs_sales.c.cashbox == self.segment_obj.cashbox_id))
        )
        rows = await database.fetch_all(query)
        return [row.chat_id for row in rows]