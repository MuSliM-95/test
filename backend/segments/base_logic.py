from abc import ABC, abstractmethod

from database.db import database, segments


class BaseSegmentLogic(ABC):
    def __init__(self, segment_obj):
        self.segment_obj = segment_obj

    async def refresh_segment_obj(self):
        self.segment_obj = await database.fetch_one(
            segments.select().where(segments.c.id == self.segment_obj.id))

    @abstractmethod
    async def update_segment(self):
        """Метод для обновлени сегмента. Должен быть переопредел в каждой логике"""
        pass

    @abstractmethod
    async def collect_data(self):
        """Метод для сбора данных сегмента"""
        pass

    @abstractmethod
    async def start_actions(self):
        """Метод для запуска actions"""
        pass