from database.db import (
    database, segments, segment_version_objects, segment_versions,
    SegmentObjectType, SegmentChangeType
)
from sqlalchemy import select

from segments.helpers.collect_obj_ids import collect_objects
from sqlalchemy.dialects.postgresql import insert


class SegmentLogic:
    """
    Класс для создания логики обновления сегментов.
    """

    def __init__(self, segment_obj):
        self.segment_obj = segment_obj

    async def collect_id_changes(self, new_ids: dict):
        """Сбор всех изменений относительно последнего среза"""

        docs_sales_id_to_update = new_ids.get('docs_sales', [])
        contragents_id_to_update = new_ids.get('contragents', [])

        docs_sales_ids = await collect_objects(self.segment_obj.id, self.segment_obj.current_version, SegmentObjectType.docs_sales.value, SegmentChangeType.existing.value)

        contragents_ids = await collect_objects(self.segment_obj.id, self.segment_obj.current_version, SegmentObjectType.contragents.value, SegmentChangeType.existing.value)

        changes = {
            SegmentObjectType.docs_sales.value: {},
            SegmentObjectType.contragents.value: {}
        }

        # собираем изменения документов продаж
        changes[SegmentObjectType.docs_sales.value][SegmentChangeType.added.value] = list(
            set(docs_sales_id_to_update) - set(docs_sales_ids))
        changes[SegmentObjectType.docs_sales.value][SegmentChangeType.removed.value] = list(
            set(docs_sales_ids) - set(docs_sales_id_to_update))
        changes[SegmentObjectType.docs_sales.value][SegmentChangeType.existing.value] = docs_sales_id_to_update

        # собираем изменения контрагентов
        changes[SegmentObjectType.contragents.value][SegmentChangeType.added.value] = list(
            set(contragents_id_to_update) - set(contragents_ids))
        changes[SegmentObjectType.contragents.value][SegmentChangeType.removed.value] = list(
            set(contragents_ids) - set(contragents_id_to_update))
        changes[SegmentObjectType.contragents.value][SegmentChangeType.existing.value] = contragents_id_to_update

        return changes

    async def create_segment_version(self, conn):
        """Создание новой версии среза"""

        current_version = self.segment_obj.current_version or 0
        query = segment_versions.insert().values(
            version=current_version + 1,
            segment_id=self.segment_obj.id
        ).returning(segment_versions.c.id)

        return await conn.fetch_one(query)

    async def update_version_data(self, new_version_id: int, changes: dict, conn):
        """Метод для записи среза (изменненых id объектов) в сегменте."""
        values = []
        for object_type, value_data in changes.items():
            for change_type, ids in value_data.items():
                for idx in ids:
                    values.append({
                        "segment_id": self.segment_obj.id,
                        "version": new_version_id,
                        "object_id": idx,
                        "object_type": object_type,
                        "change_type": change_type
                    })
        if not values:
            return
        query = insert(segment_version_objects).values(values)
        query = query.on_conflict_do_nothing(
            index_elements=["segment_id", "version", "object_id", "object_type", "change_type"]
        )
        await conn.execute(query)
        return

    async def update_segment_data_in_db(self, new_ids: dict):
        """Обновление в БД"""

        changes = await self.collect_id_changes(new_ids)

        async with database.connection() as connection:
            async with connection.transaction():
                new_version = await self.create_segment_version(connection)
                await self.update_version_data(new_version.id, changes, connection)
                current_version = self.segment_obj.current_version or 0
                query = segments.update().where(segments.c.id == self.segment_obj.id).values(current_version=current_version + 1)
                await connection.execute(query)

