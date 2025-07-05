import json
from abc import ABC, abstractmethod
from typing import List

from database.db import database, segments


class BaseSegmentLogic(ABC):
    """
    Абстрактный класс для создания логики сегментов.
    ACTIONS должен переопределяться если в определнной логике
    есть какие либо actions
    """

    def __init__(self, segment_obj):
        self.segment_obj = segment_obj
        self.ACTIONS = {}

    async def refresh_segment_obj(self):
        self.segment_obj = await database.fetch_one(
            segments.select().where(segments.c.id == self.segment_obj.id))

    async def update_id_in_segment(self, id_to_update: List[int]):
        id_in_segment = self.segment_obj.current_ids if self.segment_obj.current_ids is not None else []
        ids_to_segment = list(
            set(id_to_update) - set(id_in_segment))
        ids_out_of_segment = list(
            set(id_in_segment) - set(id_to_update))
        changes = {}
        changes["last_added_ids"] = ids_to_segment
        changes["last_removed_ids"] = ids_out_of_segment
        if ids_to_segment or ids_out_of_segment:
            changes["current_ids"] = id_to_update
        if changes:
            await database.execute(
                segments.update().where(segments.c.id == self.segment_obj.id)
                .values(
                    **changes
                )
            )

    @abstractmethod
    async def update_segment(self):
        """Метод для обновлени сегмента. Должен быть переопредел в каждой логике"""
        pass

    @abstractmethod
    async def collect_data(self):
        """Метод для сбора данных сегмента"""
        pass

    @abstractmethod
    async def run_action(self, action: str, ids: List[int], data: dict = None):
        """Метод для выполения action"""
        pass

    async def start_actions(self):
        """Метод для запуска actions"""
        await self.refresh_segment_obj()
        actions = json.loads(self.segment_obj.actions)
        if not actions or not isinstance(actions, dict):
            return
        for k, v in actions.items():
            ids = self.segment_obj.current_ids if self.segment_obj.current_ids is not None else []
            if v.get('trigger_on_new'):
                del v['trigger_on_new']
                ids = self.segment_obj.last_added_ids if self.segment_obj.last_added_ids is not None else []
            if v.get('trigger_on_removed'):
                ids = self.segment_obj.last_removed_ids if self.segment_obj.last_removed_ids is not None else []
                del v['trigger_on_removed']
            await self.run_action(k, ids, v)
        return

