from typing import Optional, Annotated

from fastapi import Query
from sqlalchemy import select, func

from api.categories.routers import build_hierarchy
from common.s3_service.core.IS3ServiceFactory import IS3ServiceFactory
from database.db import categories, database, nomenclature, pictures
from functions.helpers import get_user_by_token, datetime_to_timestamp


class GetCategoriesTreeView:
    def add_picture_url_recursive(self, node, api_url):
        if node.get('picture'):
            node['picture_url'] = f"{api_url}/api/photos/{node['picture']}"
        else:
            node['picture_url'] = None
        if 'children' in node and isinstance(node['children'], list):
            for child in node['children']:
                self.add_picture_url_recursive(child, api_url)

    def __init__(
        self,
        s3_factory: IS3ServiceFactory
    ):
        self.__s3_factory = s3_factory

    async def __call__(
        self,
        token: str, nomenclature_name: Optional[str] = None,
        offset: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=100)] = 100,
        include_photo: bool = True
    ):
        """Получение древа списка категорий"""
        user = await get_user_by_token(token)

        if include_photo:
            # Если нужны фото, делаем JOIN с pictures
            query = (
                select(
                    categories,
                    pictures.c.url.label("picture")
                )
                .select_from(categories)
                .outerjoin(pictures, categories.c.photo_id == pictures.c.id)
                .where(
                    categories.c.cashbox == user.cashbox_id,
                    categories.c.is_deleted.is_not(True),
                    categories.c.parent is None
                )
                .limit(limit)
                .offset(offset)
            )
        else:
            # Без фото - не делаем JOIN
            query = (
                select(categories)
                .where(
                    categories.c.cashbox == user.cashbox_id,
                    categories.c.is_deleted.is_not(True),
                    categories.c.parent is None
                )
                .limit(limit)
                .offset(offset)
            )

        categories_db = await database.fetch_all(query)
        result = []

        nomenclature_list = []
        if nomenclature_name is not None:
            q = nomenclature.select().where(
                nomenclature.c.name.ilike(f"%{nomenclature_name}%"),
                nomenclature.c.category is not None
            )
            nomenclature_list = await database.fetch_all(q)

        import os
        API_URL = os.getenv("API_URL", "http://localhost:7000")
        for category in categories_db:
            category_dict = dict(category)
            category_dict['key'] = category_dict['id']

            nomenclature_in_category = (
                select(
                    func.count(nomenclature.c.id).label("nom_count")
                )
                .where(
                    nomenclature.c.category == category.get("id"),
                    (
                        nomenclature.c.name.ilike(f"%{nomenclature_name}%")
                        if nomenclature_name else True
                    )
                )
                .group_by(nomenclature.c.category)
            )
            nomenclature_in_category_result = await database.fetch_one(
                nomenclature_in_category
            )
            category_dict["nom_count"] = (
                0 if not nomenclature_in_category_result
                else nomenclature_in_category_result.nom_count
            )

            category_dict['expanded_flag'] = False

            if include_photo:
                query = (
                    f"""
with recursive categories_hierarchy as (
        select id, name, parent, description, code, status, updated_at, created_at, photo_id,
            1 as lvl
    from categories where parent = {category.id}
    union
        select F.id, F.name, F.parent, F.description, F.code, F.status, F.updated_at,
            F.created_at, F.photo_id, H.lvl+1
    from categories_hierarchy as H
    join categories as F on F.parent = H.id
)
select ch.*, p.url as picture from categories_hierarchy ch
left join pictures p on ch.photo_id = p.id
"""
                )
            else:
                query = (
                    f"""
with recursive categories_hierarchy as (
        select id, name, parent, description, code, status, updated_at, created_at, photo_id,
            1 as lvl
    from categories where parent = {category.id}
    union
        select F.id, F.name, F.parent, F.description, F.code, F.status, F.updated_at,
            F.created_at, F.photo_id, H.lvl+1
    from categories_hierarchy as H
    join categories as F on F.parent = H.id
)
select ch.* from categories_hierarchy ch
"""
                )

            childrens = await database.fetch_all(query)
            if childrens:
                category_dict['children'] = await build_hierarchy(
                    [dict(child) for child in childrens],
                    category.id,
                    nomenclature_name
                )
            else:
                category_dict['children'] = []

            flag = True
            if nomenclature_name is not None:
                flag = False
                cats_ids = [child.parent for child in childrens]
                if len(childrens) != 0:
                    cats_ids.append(childrens[-1].id)
                else:
                    cats_ids = [category.id]
                for nomenclature_entity in nomenclature_list:
                    if nomenclature_entity.category in cats_ids:
                        flag = True
                        break

            if flag is True:
                # Рекурсивно добавить picture_url для всех детей
                self.add_picture_url_recursive(category_dict, API_URL)
                result.append(category_dict)

        categories_db = [*map(datetime_to_timestamp, result)]

        query = select(func.count(categories.c.id)).where(
            categories.c.cashbox == user.cashbox_id,
            categories.c.is_deleted.is_not(True),
        )

        categories_db_count = await database.fetch_one(query)
        return {"result": categories_db, "count": categories_db_count.count_1}
