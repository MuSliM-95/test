import json
from datetime import datetime
from typing import List

from database.db import (
    database, segments, tags, contragents_tags, SegmentObjectType, users,
    users_cboxes_relation, docs_sales, docs_sales_tags, employee_shifts
)
from sqlalchemy import select, and_, func, literal, or_
from sqlalchemy.dialects.postgresql import insert

from segments.actions.segment_tg_notification import send_segment_notification
from segments.masks import replace_masks

from segments.helpers.collect_obj_ids import collect_objects

from segments.constants import SegmentChangeType

from segments.helpers.functions import create_replacements


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

    def _check_recipient_conditions(self, data: dict):
        """
        Проверяет, соответствует ли текущее время всем заданным условиям.
        Возвращает True, если все условия выполнены или не заданы.
        """

        now = datetime.now()

        # Проверка временного диапазона
        if data.get("time_range"):
            time_range = data["time_range"]
            current_time = now.time()

            # Парсим время начала и конца
            from_time = datetime.strptime(time_range["from_"], "%H:%M").time()
            to_time = datetime.strptime(time_range["to_"], "%H:%M").time()

            # Проверяем, попадает ли текущее время в диапазон
            if from_time <= to_time:
                # Обычный случай: диапазон в пределах одних суток (например, 09:00-17:00)
                if not (from_time <= current_time <= to_time):
                    return False
            else:
                # Диапазон через полночь (например, 22:00-06:00)
                if not (current_time >= from_time or current_time <= to_time):
                    return False

        # Проверка дней недели (1=понедельник, 7=воскресенье)
        if data.get("weekdays"):
            current_weekday = now.isoweekday()  # 1=понедельник, 7=воскресенье
            if current_weekday not in data["weekdays"]:
                return False

        # Проверка дней месяца
        if data.get("month_days"):
            current_day = now.day
            if current_day not in data["month_days"]:
                return False

        # Проверка модуло дня месяца
        if data.get("month_day_modulo"):
            modulo = data["month_day_modulo"]
            current_day = now.day
            if current_day % modulo["divisor"] != modulo["remainder"]:
                return False

        return True


    async def send_tg_notification(self, order_ids:List[int], data: dict):
        chat_ids = set()
        message = data.get("message")
        send_to = data.get("send_to")
        user_tag = data.get("user_tag")
        recipients = data.get("recipients")
        shift_status = data.get("shift_status")
        if not message or (not send_to and not user_tag and not recipients):
            return
        if user_tag:
            chat_ids.update(await self.get_user_chat_ids_by_tag(user_tag, shift_status))

        if recipients:
            for recipient in recipients:
                if self._check_recipient_conditions(recipient.get("conditions", {})):
                    chat_ids.update(
                        await self.get_user_chat_ids_by_tag(recipient.get("user_tag"), recipient.get("shift_status"))
                    )

        for order_id in order_ids:
            message_text = f'Заказ # - {str(order_id)}\n\n' + message

            replacements = await create_replacements(order_id)

            message_text = replace_masks(message_text, replacements)
            if send_to == "picker":
                chat_ids.update(await self.get_picker_chat_id(order_id))
            elif send_to == "courier":
                chat_ids.update(await self.get_courier_chat_id(order_id))
            if not chat_ids:
                return False
            await send_segment_notification(
                recipient_ids=list(chat_ids),
                notification_text=message_text,
                segment_id=self.segment_obj.id,
            )

    async def get_user_chat_ids_by_tag(self, user_tag: str, shift_status: str = None):
        subquery = (
            select(users.c.chat_id, users_cboxes_relation.c.id.label("relation_id"), users_cboxes_relation.c.user.label("user_id"))
            .join(users_cboxes_relation,
                  users_cboxes_relation.c.user == users.c.id)
            .where(and_(
                users_cboxes_relation.c.cashbox_id == self.segment_obj.cashbox_id,
                literal(user_tag) == func.any(users_cboxes_relation.c.tags)
            ))
        ).subquery("sub")
        query = select(subquery.c.chat_id)
        if shift_status:
            if shift_status == 'off_shift':
                where_clause = or_(
                        employee_shifts.c.user_id.is_(None),
                        employee_shifts.c.status == shift_status
                    )
            else:
                where_clause = or_(
                        employee_shifts.c.status == shift_status
                    )
            query = (
                query
                .outerjoin(employee_shifts, subquery.c.user_id == employee_shifts.c.user_id)
                .where(where_clause)
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