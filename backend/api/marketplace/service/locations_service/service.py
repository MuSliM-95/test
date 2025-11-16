from sqlalchemy import select, and_, func, literal_column

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.locations_service.schemas import LocationsListResponse, LocationsListRequest
from database.db import warehouses, database, marketplace_rating_aggregates


class MarketplaceLocationsService(BaseMarketplaceService):
    async def get_locations(
            self,
            request: LocationsListRequest
    ) -> LocationsListResponse:
        lat = request.lat
        lon = request.lon
        radius = request.radius
        page = request.page
        size = request.size

        offset = (page - 1) * size

        # Основной запрос для получения информации о складах
        query = select(
            warehouses.c.id,
            warehouses.c.name,
            warehouses.c.address,
            warehouses.c.latitude,
            warehouses.c.longitude,
            warehouses.c.description,
            # Join с рейтингами для получения avg_rating и reviews_count
            marketplace_rating_aggregates.c.avg_rating,
            marketplace_rating_aggregates.c.reviews_count
        ).select_from(
            warehouses.outerjoin(
                marketplace_rating_aggregates,
                and_(
                    marketplace_rating_aggregates.c.entity_id == warehouses.c.id,
                    marketplace_rating_aggregates.c.entity_type == "warehouse"
                )
            )
        )

        # Условия для фильтрации складов
        conditions = [
            warehouses.c.is_public.is_(True),
            warehouses.c.status.is_(True),
            warehouses.c.is_deleted.is_not(True)
        ]

        # Если предоставлены координаты и радиус, добавляем фильтр по расстоянию
        if lat is not None and lon is not None and radius is not None:
            # Используем формулу Haversine для PostgreSQL
            # earth_radius = 6371 (км)
            distance_expr = func.acos(
                func.cos(func.radians(lat)) *
                func.cos(func.radians(warehouses.c.latitude)) *
                func.cos(func.radians(warehouses.c.longitude) - func.radians(lon)) +
                func.sin(func.radians(lat)) *
                func.sin(func.radians(warehouses.c.latitude))
            ) * 6371

            # Добавляем условие фильтрации по радиусу
            conditions.append(distance_expr <= radius)
            # Добавляем вычисляемое поле расстояния для сортировки и отображения
            query = query.add_columns(distance_expr.label('distance'))
            # Сортировка по расстоянию (ближайшие первыми)
            query = query.order_by(distance_expr)
        else:
            # Если нет координат, сортируем по ID
            query = query.order_by(warehouses.c.id)
            # Добавляем NULL для distance
            query = query.add_columns(literal_column('NULL').label('distance'))

        # Строим запрос с условиями
        query = query.where(and_(*conditions))

        # Запрос для подсчета общего количества записей с теми же условиями
        count_query = select(func.count(warehouses.c.id)).select_from(
            warehouses.outerjoin(
                marketplace_rating_aggregates,
                and_(
                    marketplace_rating_aggregates.c.entity_id == warehouses.c.id,
                    marketplace_rating_aggregates.c.entity_type == "warehouse"
                )
            )
        ).where(and_(*conditions))

        total_count = await database.fetch_val(count_query)

        # Применяем пагинацию
        query = query.limit(size).offset(offset)
        locations_db = await database.fetch_all(query)

        # Обрабатываем результаты
        locations = []
        for location in locations_db:
            loc_dict = dict(location)

            # Преобразуем distance в float если оно не None
            if 'distance' in loc_dict and loc_dict['distance'] is not None:
                try:
                    loc_dict['distance'] = float(loc_dict['distance'])
                except (TypeError, ValueError):
                    loc_dict['distance'] = None
            elif lat is not None and lon is not None:
                loc_dict['distance'] = self._count_distance_to_client(lat, lon, loc_dict['latitude'], loc_dict['longitude'])
            locations.append(loc_dict)

        return LocationsListResponse(**{
            "locations": locations,
            "count": total_count,
            "page": page,
            "size": size
        })