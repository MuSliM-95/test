import json
from typing import List

from database.db import (
    database, segments, tags, contragents_tags, SegmentObjectType, users,
    users_cboxes_relation, docs_sales, docs_sales_tags
)
from sqlalchemy import select, and_, func, literal
from sqlalchemy.dialects.postgresql import insert

from segments.actions.segment_tg_notification import send_segment_notification
from segments.masks import replace_masks

from segments.helpers.collect_obj_ids import collect_objects

from segments.constants import SegmentChangeType

from segments.helpers.functions import create_replacements

from api.employee_shifts.service import (
    check_user_on_shift,
    get_available_pickers_on_shift,
    get_available_couriers_on_shift
)


class SegmentActions:
    def __init__(self, segment_obj):
        self.segment_obj = segment_obj
        self.ACTIONS = {
            "add_existed_tags": {
                "obj_type": SegmentObjectType.contragents.value,
                "method": self.add_existed_tags
            },
            "remove_tags": {
                "obj_type": SegmentObjectType.contragents.value,
                "method": self.remove_tags
            },
            "client_tags": {
                "obj_type": SegmentObjectType.contragents.value,
                "method": self.client_tags
            },
            "send_tg_notification": {
                "obj_type": SegmentObjectType.docs_sales.value,
                "method": self.send_tg_notification
            },
            "add_docs_sales_tags": {
                "obj_type": SegmentObjectType.docs_sales.value,
                "method": self.add_docs_sales_tags
            },
            "remove_docs_sales_tags": {
                "obj_type": SegmentObjectType.docs_sales.value,
                "method": self.remove_docs_sales_tags
            }
        }

    async def refresh_segment_obj(self):
        self.segment_obj = await database.fetch_one(
            segments.select().where(segments.c.id == self.segment_obj.id))

    async def run(self, action: str, ids: List[int],
                         data: dict = None):
        """Метод для выполения action"""
        await self.ACTIONS[action]["method"](ids, data)

    async def start_actions(self):
        """Метод для запуска actions"""
        await self.refresh_segment_obj()
        if self.segment_obj.actions is None:
            return
        actions = json.loads(self.segment_obj.actions)
        if not actions or not isinstance(actions, dict):
            return
        for k, v in actions.items():
            if k not in self.ACTIONS:
                continue
            if v.get('trigger_on_new'):
                del v['trigger_on_new']
                ids = await collect_objects(self.segment_obj.id, self.ACTIONS[k]["obj_type"], SegmentChangeType.new.value)
            elif v.get('trigger_on_removed'):
                ids = await collect_objects(self.segment_obj.id, self.ACTIONS[k]["obj_type"], SegmentChangeType.removed.value)
                del v['trigger_on_removed']
            else:
                ids = await collect_objects(
                    self.segment_obj.id,
                    self.ACTIONS[k]["obj_type"],
                    SegmentChangeType.active.value)
            if ids:
                await self.run(k, ids, v)
        return

    async def add_existed_tags(self, contragents_ids: List[int], data: dict):
        tag_names = data.get("name", [])
        query = select(tags.c.id).where(and_(tags.c.name.in_(tag_names), tags.c.cashbox_id == self.segment_obj.cashbox_id))
        rows = await database.fetch_all(query)
        tag_ids = [row.id for row in rows]
        new_values = []
        for tag_id in tag_ids:
            for contragent_id in contragents_ids:
                new_values.append({"contragent_id": contragent_id, "tag_id": tag_id, "cashbox_id": self.segment_obj.cashbox_id})
        if not new_values:
            return
        query = insert(contragents_tags).values(new_values)
        query = query.on_conflict_do_nothing(
            index_elements=["tag_id", "contragent_id"]  # <- уникальная пара
        )
        await database.execute(query)

    async def remove_tags(self, contragents_ids: List[int], data: dict):
        tag_names = data.get("name", [])
        query = select(tags.c.id).where(tags.c.name.in_(tag_names), tags.c.cashbox_id == self.segment_obj.cashbox_id)
        rows = await database.fetch_all(query)
        tag_ids = [row.id for row in rows]
        query = (
            contragents_tags.delete()
            .where(contragents_tags.c.tag_id.in_(tag_ids), contragents_tags.c.contragent_id.in_(contragents_ids))
        )
        await database.execute(query)

    async def client_tags(self, contragents_ids: List[int], data: dict):
        names = []
        prepared_data = []
        tags_data = data.get("tags", [])
        for d in tags_data:
            names.append(d["name"])
            prepared_data.append({
                "name": d["name"],
                "emoji": d.get("emoji", None),
                "color": d.get("color", None),
                "description": d.get("description", None),
                "cashbox_id": self.segment_obj.cashbox_id,
            })

        count_query = (
            select(func.count())
            .select_from(tags)
            .where(tags.c.name.in_(names), tags.c.cashbox_id == self.segment_obj.cashbox_id)
        )

        count_rows = await database.execute(count_query)

        if count_rows != len(set(names)):
            insert_query = insert(tags).values(prepared_data).on_conflict_do_nothing(index_elements=['name', "cashbox_id"])
            await database.execute(insert_query)

        await self.add_existed_tags(contragents_ids, {"name": names})

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

            replacements = await create_replacements(order_id)

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

    async def get_user_chat_ids_by_tag(self, user_tag: str):
        query = (
            select(users.c.chat_id)
            .join(users_cboxes_relation,
                  users_cboxes_relation.c.user == users.c.id)
            .where(and_(
                users_cboxes_relation.c.cashbox_id == self.segment_obj.cashbox_id,
                literal(user_tag) == func.any(users_cboxes_relation.c.tags)
            ))
        )
        rows = await database.fetch_all(query)
        return [row.chat_id for row in rows]

    async def get_picker_chat_id(self, order_id: int):
        order_query = docs_sales.select().where(
            and_(docs_sales.c.id == order_id, docs_sales.c.cashbox == self.segment_obj.cashbox_id)
        )
        order = await database.fetch_one(order_query)
        
        if not order:
            return []
        
        chat_ids = []
        
        if order.assigned_picker:
            if await check_user_on_shift(order.assigned_picker):
                query = (
                    select(users.c.chat_id)
                    .join(users_cboxes_relation, users_cboxes_relation.c.user == users.c.id)
                    .where(users_cboxes_relation.c.id == order.assigned_picker)
                )
                picker = await database.fetch_one(query)
                if picker and picker.chat_id:
                    chat_ids.append(picker.chat_id)
        
        # Если нет назначенного сборщика или он не на смене
        if not chat_ids:
            available_pickers = await get_available_pickers_on_shift(self.segment_obj.cashbox_id)
            
            if available_pickers:
                query = (
                    select(users.c.chat_id)
                    .join(users_cboxes_relation, users_cboxes_relation.c.user == users.c.id)
                    .where(
                        and_(
                            users_cboxes_relation.c.id.in_(available_pickers),
                            users.c.chat_id.is_not(None)
                        )
                    )
                )
                rows = await database.fetch_all(query)
                chat_ids = [row.chat_id for row in rows if row.chat_id]
        
        return chat_ids


    async def get_courier_chat_id(self, order_id: int):
        order_query = docs_sales.select().where(
            and_(docs_sales.c.id == order_id, docs_sales.c.cashbox == self.segment_obj.cashbox_id)
        )
        order = await database.fetch_one(order_query)
        
        if not order:
            return []
        
        chat_ids = []
        
        if order.assigned_courier:
            if await check_user_on_shift(order.assigned_courier):
                query = (
                    select(users.c.chat_id)
                    .join(users_cboxes_relation, users_cboxes_relation.c.user == users.c.id)
                    .where(users_cboxes_relation.c.id == order.assigned_courier)
                )
                courier = await database.fetch_one(query)
                if courier and courier.chat_id:
                    chat_ids.append(courier.chat_id)
        
        # Если нет назначенного курьера или он не на смене
        if not chat_ids:
            available_couriers = await get_available_couriers_on_shift(self.segment_obj.cashbox_id)
            
            if available_couriers:
                query = (
                    select(users.c.chat_id)
                    .join(users_cboxes_relation, users_cboxes_relation.c.user == users.c.id)
                    .where(
                        and_(
                            users_cboxes_relation.c.id.in_(available_couriers),
                            users.c.chat_id.is_not(None)
                        )
                    )
                )
                rows = await database.fetch_all(query)
                chat_ids = [row.chat_id for row in rows if row.chat_id]
        
        return chat_ids

    async def add_docs_sales_tags(self, docs_ids:List[int], data: dict):

        tags = data.get("tags")
        prepared_data = []

        for doc_id in docs_ids:
            for tag in set(tags):
                prepared_data.append({
                    "docs_sales_id": doc_id,
                    "name": tag
                })

        if prepared_data:
            query = insert(docs_sales_tags).values(prepared_data)
            await database.execute(query)

    async def remove_docs_sales_tags(self, docs_ids:List[int], data: dict):
        tags = data.get("tags")
        query = docs_sales_tags.delete().where(and_(
            docs_sales_tags.c.docs_sales_id.in_(docs_ids),
            docs_sales_tags.c.name.in_(tags)
        ))

        await database.execute(query)