from enum import Enum


class QrEntityTypes(Enum):
    NOMENCLATURE = "nomenclature"          # Товар
    WAREHOUSE = "warehouse"        # Локация/магазин
    # ORDER = "order"              # Заказ
#
#
# # Статусы отзывов
# class ReviewStatus:
#     PENDING = "pending"          # Отзыв на модерации
#     VISIBLE = "visible"          # Отзыв опубликован
#     HIDDEN = "hidden"            # Отзыв скрыт
#     REJECTED = "rejected"        # Отзыв отклонен
#
#
# # Типы отзывов
# class ReviewType:
#     LOCATION = "location"        # Отзыв на локацию
#     ORDER = "order"              # Отзыв на заказ
#     ITEM = "item"                # Отзыв на товар
#
#
# # Сортировка товаров
# class SortOptions:
#     PRICE_ASC = "price"         # По цене (возрастание)
#     PRICE_DESC = "price_desc"   # По цене (убывание)
#     NAME_ASC = "name"           # По названию (А-Я)
#     NAME_DESC = "name_desc"     # По названию (Я-А)
#     CREATED_ASC = "created_at"  # По дате создания (старые сначала)
#     CREATED_DESC = "created_desc"  # По дате создания (новые сначала)
#     RATING_DESC = "rating"      # По рейтингу (высокий сначала)
#     DISTANCE_ASC = "distance"   # По расстоянию (близкие сначала)
#
#
# # Сортировка отзывов
# class ReviewSortOptions:
#     NEWEST = "newest"           # Новые сначала
#     OLDEST = "oldest"           # Старые сначала
#     HIGHEST = "highest"         # Высокий рейтинг сначала
#     LOWEST = "lowest"           # Низкий рейтинг сначала
#
#
# # Лимиты и ограничения
# class Limits:
#     MAX_REVIEW_TEXT_LENGTH = 1000    # Максимальная длина текста отзыва
#     MIN_REVIEW_TEXT_LENGTH = 10      # Минимальная длина текста отзыва
#     MAX_RATING = 5                   # Максимальный рейтинг
#     MIN_RATING = 1                   # Минимальный рейтинг
#     MAX_PAGE_SIZE = 100              # Максимальный размер страницы
#     MIN_PAGE_SIZE = 1                # Минимальный размер страницы
#     DEFAULT_PAGE_SIZE = 20           # Размер страницы по умолчанию
#     REVIEW_COOLDOWN_HOURS = 24       # Время между отзывами от одного пользователя (часы)
#
#
# # UTM метки для аналитики
# class UTMSources:
#     MOBILE_APP = "mobile_app"        # Мобильное приложение
#     WEB_APP = "web_app"              # Веб-приложение
#     TELEGRAM_BOT = "telegram_bot"    # Telegram бот
#     QR_CODE = "qr_code"              # QR-код
#     DIRECT = "direct"                # Прямой переход
#
#
# # Стратегии распределения заказов
# class DistributionStrategy:
#     NEAREST_VIABLE_WITH_STOCK = "nearest_viable_with_stock"  # Ближайший с наличием
#     ROUND_ROBIN = "round_robin"                              # По очереди
#     LOAD_BALANCING = "load_balancing"                        # Балансировка нагрузки
#     GEOGRAPHIC = "geographic"                                # Географическое распределение
#
#
# # Статусы очереди отзывов
# class ReviewQueueStatus:
#     PENDING = "pending"          # Ожидает обработки
#     PROCESSING = "processing"    # В обработке
#     COMPLETED = "completed"      # Обработан успешно
#     FAILED = "failed"            # Ошибка обработки
#
#
# # Сообщения об ошибках
# class ErrorMessages:
#     PRODUCT_NOT_FOUND = "Товар не найден или не доступен"
#     LOCATION_NOT_FOUND = "Локация не найдена или не доступна"
#     QR_CODE_NOT_FOUND = "QR-код не найден или неактивен"
#     INVALID_ENTITY_TYPE = "Неизвестный тип сущности"
#     INVALID_RATING = "Рейтинг должен быть от 1 до 5"
#     REVIEW_TEXT_TOO_SHORT = "Текст отзыва должен содержать минимум 10 символов"
#     REVIEW_TEXT_TOO_LONG = "Текст отзыва не должен превышать 1000 символов"
#     REVIEW_COOLDOWN = "Можно оставить только один отзыв в сутки"
#     ALREADY_IN_FAVORITES = "Элемент уже добавлен в избранное"
#     FAVORITE_NOT_FOUND = "Элемент не найден в избранном"
#     ORDER_QUEUE_FAILED = "Ошибка при обработке заказа"
#     INVALID_ENTITY_TYPE_FAVORITE = "Тип сущности должен быть 'product' или 'location'"
#     INVALID_ORDER_TYPE = "Неизвестный тип заказа"
#     INVALID_DISTRIBUTION_STRATEGY = "Неизвестная стратегия распределения"
#     INVALID_REVIEW_TYPE = "Неизвестный тип отзыва"
#     CASHBOX_NOT_FOUND = "Касса не найдена или не доступна"
#     WAREHOUSE_NOT_FOUND = "Склад не найден или не доступен"
#     INSUFFICIENT_STOCK = "Недостаточно товара на складе"
#
#
# # Настройки по умолчанию
# class DefaultSettings:
#     DEFAULT_DISTRIBUTION_STRATEGY = DistributionStrategy.NEAREST_VIABLE_WITH_STOCK
#     DEFAULT_ORDER_TYPE = OrderType.SELF
#     DEFAULT_DELIVERY_TYPE = DeliveryType.PICKUP
#     DEFAULT_REVIEW_STATUS = ReviewStatus.PENDING
#     DEFAULT_PAGE_SIZE = Limits.DEFAULT_PAGE_SIZE
#     DEFAULT_SORT_OPTION = SortOptions.CREATED_DESC
#
#
# # Валидация
# class Validation:
#     PHONE_REGEX = r"^\+?[1-9]\d{1,14}$"  # Простая валидация телефона
#     EMAIL_REGEX = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
#     MIN_PASSWORD_LENGTH = 8
#     MAX_PASSWORD_LENGTH = 128
#
#
# # Кэширование
# class CacheKeys:
#     PRODUCTS_PREFIX = "mp:products:"
#     LOCATIONS_PREFIX = "mp:locations:"
#     REVIEWS_PREFIX = "mp:reviews:"
#     RATINGS_PREFIX = "mp:ratings:"
#     CACHE_TTL = 300  # 5 минут
#
#
# # WebSocket события
# class WebSocketEvents:
#     ORDER_STATUS_CHANGED = "order_status_changed"
#     REVIEW_ADDED = "review_added"
#     RATING_UPDATED = "rating_updated"
#     STOCK_CHANGED = "stock_changed"
#
#
# # Логирование
# class LogLevels:
#     DEBUG = "DEBUG"
#     INFO = "INFO"
#     WARNING = "WARNING"
#     ERROR = "ERROR"
#     CRITICAL = "CRITICAL"
#
#
# # Метаданные для аналитики
# class AnalyticsMetadata:
#     SOURCE_MOBILE = "mobile"
#     SOURCE_WEB = "web"
#     SOURCE_API = "api"
#     SOURCE_TELEGRAM = "telegram"
#
#     ACTION_VIEW = "view"
#     ACTION_FAVORITE = "favorite"
#     ACTION_ORDER = "order"
#     ACTION_REVIEW = "review"
#     ACTION_QR_SCAN = "qr_scan"