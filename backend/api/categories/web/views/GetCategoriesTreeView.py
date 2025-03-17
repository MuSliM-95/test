from typing import Optional

from sqlalchemy import select, func

from api.categories.routers import build_hierarchy
from common.s3_service.core.IS3ServiceFactory import IS3ServiceFactory
from database.db import categories, database, nomenclature, pictures
from functions.helpers import get_user_by_token, datetime_to_timestamp


class GetCategoriesTreeView:

    def __init__(
        self,
        s3_factory: IS3ServiceFactory
    ):
        self.__s3_factory = s3_factory

    async def __call__(
        self,
        token: str, nomenclature_name: Optional[str] = None
    ):
        """Получение древа списка категорий"""
        user = await get_user_by_token(token)

        s3_client = self.__s3_factory()

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
                categories.c.parent == None
            )
        )

        categories_db = await database.fetch_all(query)
        result = []

        nomenclature_list = []
        if nomenclature_name != None:
            q = nomenclature.select().where(nomenclature.c.name.ilike(f"%{nomenclature_name}%"),
                                            nomenclature.c.category != None)
            nomenclature_list = await database.fetch_all(q)

        for category in categories_db:
            category_dict = dict(category)
            category_dict['key'] = category_dict['id']

            try:
                if category_dict.get("picture"):
                    url = await s3_client.get_link_object(
                        bucket_name="5075293c-docs_generated",
                        file_key=category_dict.get("picture")
                    )
                    category_dict["picture"] = url
            except Exception as e:
                print(e)

            if nomenclature_name is not None:
                nomenclature_in_category = await database.fetch_all(
                    nomenclature.select().
                    where(nomenclature.c.name.ilike(f"%{nomenclature_name}%"),
                          nomenclature.c.category == category.get("id")))
                category_dict["nom_count"] = len(nomenclature_in_category)
            else:
                category_dict["nom_count"] = 0

            category_dict['expanded_flag'] = False
            query = (
                f"""
                        with recursive categories_hierarchy as (
                        select id, name, parent, description, code, status, updated_at, created_at, photo_id, 1 as lvl
                        from categories where parent = {category.id}

                        union
                        select F.id, F.name, F.parent, F.description, F.code, F.status, F.updated_at, F.created_at, F.photo_id, H.lvl+1
                        from categories_hierarchy as H
                        join categories as F on F.parent = H.id
                        ) 
                        select ch.*, p.url as picture from categories_hierarchy ch
                        left join pictures p on ch.photo_id = p.id
                    """
            )
            childrens = await database.fetch_all(query)
            if childrens:
                category_dict['children'] = await build_hierarchy([dict(child) for child in childrens], category.id,
                                                                  nomenclature_name)
                for element in category_dict['children']:
                    try:
                        if element.get("picture"):
                            url = await s3_client.get_link_object(
                                bucket_name="5075293c-docs_generated",
                                file_key=element.get("picture")
                            )
                            element["picture"] = url
                    except Exception as e:
                        print(e)
            else:
                category_dict['children'] = []

            flag = True
            if nomenclature_name != None:
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
                result.append(category_dict)

        categories_db = [*map(datetime_to_timestamp, result)]

        query = select(func.count(categories.c.id)).where(
            categories.c.cashbox == user.cashbox_id,
            categories.c.is_deleted.is_not(True),
        )

        categories_db_count = await database.fetch_one(query)
        return {"result": categories_db, "count": categories_db_count.count_1}