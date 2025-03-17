from sqlalchemy import select, func

from common.s3_service.core.IS3ServiceFactory import IS3ServiceFactory
from database.db import categories, pictures, database
from functions.helpers import get_user_by_token, datetime_to_timestamp


class GetAllCategoriesView:

    def __init__(
        self,
        s3_factory: IS3ServiceFactory
    ):
        self.__s3_factory = s3_factory

    async def __call__(
        self,
        token: str, limit: int = 100, offset: int = 0
    ):
        """Получение списка категорий"""
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
            )
            .limit(limit)
            .offset(offset)
        )

        categories_db = await database.fetch_all(query)
        categories_db = [*map(datetime_to_timestamp, categories_db)]
        for category in categories_db:
            if category.get("picture"):
                try:
                    url = await s3_client.get_link_object(
                        bucket_name="5075293c-docs_generated",
                        file_key=category.get("picture")
                    )
                    category["picture"] = url
                except Exception as e:
                    print(e)

        query = select(func.count(categories.c.id)).where(
            categories.c.owner == user.id,
            categories.c.is_deleted.is_not(True),
        )

        categories_db_count = await database.fetch_one(query)

        return {"result": categories_db, "count": categories_db_count.count_1}