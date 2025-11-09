from api.marketplace.service.favorites_service.service import MarketplaceFavoritesService
from api.marketplace.service.orders_service.service import MarketplaceOrdersService
from api.marketplace.service.products_list_service.service import MarketplaceProductsListService
from api.marketplace.service.qr_service.service import MarketplaceQrService
from api.marketplace.service.review_service.service import MarketplaceReviewService
from api.marketplace.service.view_event_service.service import MarketplaceViewEventService
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.utils.ioc.ioc import ioc


class MarketplaceService(MarketplaceOrdersService, MarketplaceProductsListService, MarketplaceReviewService, MarketplaceFavoritesService, MarketplaceQrService, MarketplaceViewEventService):
    async def connect(self):
        self.__rabbitmq = await ioc.get(IRabbitFactory)()

    # @staticmethod
    # async def get_locations( # TODO: rewrite locations на warehouses balaneces
    #     city: Optional[str],
    #     lat: Optional[float],
    #     lon: Optional[float],
    #     radius: Optional[float],
    #     page: int,
    #     size: int,
    #     sort: Optional[str],
    # ):
    #     offset = (page - 1) * size
    #     query = select(
    #         warehouses.c.id,
    #         warehouses.c.name,
    #         warehouses.c.address,
    #         warehouses.c.cashbox,
    #         warehouses.c.owner,
    #         warehouses.c.created_at,
    #         warehouses.c.updated_at,
    #     ).select_from(warehouses)
    #
    #     conditions = [warehouses.c.is_public == True]
    #     # if city:
    #     #     conditions.append(cboxes.c.city.ilike(f"%{city}%"))
    #     query = query.where(and_(*conditions))
    #
    #     if sort == "name":
    #         query = query.order_by(warehouses.c.name)
    #     else:
    #         query = query.order_by(warehouses.c.name)
    #
    #     count_query = select(func.count()).select_from(warehouses).where(and_(*conditions))
    #     total_count = await database.fetch_val(count_query)
    #
    #     query = query.limit(size).offset(offset)
    #     locations = await database.fetch_all(query)
    #
    #     result: List[Dict[str, Any]] = []
    #     for location in locations:
    #         ld = dict(location)
    #         if ld.get("address"):
    #             ld["address"] = str(ld["address"])
    #         ld["cashbox_id"] = ld.get("cashbox")
    #         ld["admin_id"] = ld.get("owner")
    #         ld["avg_rating"] = None # TODO: add rating
    #         ld["reviews_count"] = 0
    #         result.append(ld)
    #
    #     return {"result": result, "count": total_count, "page": page, "size": size}
