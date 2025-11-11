# from sqlalchemy import Table, select, func
#
# from database.db import database
#
#
# class InitialHashUpdater:
#     @staticmethod
#     async def __need_hashing(table: Table, table_hash: Table):
#         table_count_query = select(func.count(table.c.id))
#         hash_count_query = select(func.count(table_hash.c.id))
#
#         table_count = await database.fetch_val(table_count_query)
#         hash_count = await database.fetch_val(hash_count_query)
#
#         return bool(table_count - hash_count)
#
#     async def __update_hashes_in_table(self, table: Table, table_hash: Table):
#         if self.__need_hashing(table, table_hash):
#             entity_ids_query = select(table.c.id)
#             entity_ids = await database.fetch_all(entity_ids_query)
#
#             for entity_id in entity_ids:
#                 await
#     async def update_hashes(self):
#         pass
# TODO: дописать..