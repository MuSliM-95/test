from typing import List

from segments.masks import replace_masks

from api.docs_sales.routers import generate_and_save_order_links

from segments.actions.segment_tg_notification import send_segment_notification
from sqlalchemy import select, and_

from database.db import users, users_cboxes_relation, database, docs_sales

from segments.base.base_action import BaseAction


class DocsSalesAction(BaseAction):
    def __init__(self, segment_obj):
        super().__init__(segment_obj)
        self.ACTIONS = {
            "send_tg_notification": self.send_tg_notification,
        }

    async def run(self, action, *args, **kwargs):
        if action in self.ACTIONS:
            await self.ACTIONS[action](*args, **kwargs)

    async def send_tg_notification(self, order_ids:List[int], data: dict):
        chat_ids = []
        message = data.get("message")
        send_to = data.get("send_to")
        user_tag = data.get("user_tag")
        if not message or (not send_to and not user_tag):
            return
        if send_to is None or send_to not in ["picker", "courier"]:
            chat_ids = await self.get_user_chat_ids_by_tag(user_tag)
        for order_id in order_ids:
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