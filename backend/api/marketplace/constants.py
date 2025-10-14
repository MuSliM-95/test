"""
Константы для модуля маркетплейса.
Содержит все константы, используемые в функциональности маркетплейса.
"""

# Статусы заказов маркетплейса
class OrderStatus:
    PENDING = "pending"           # Заказ создан, ожидает обработки
    PROCESSING = "processing"     # Заказ в обработке
    PICKING = "picking"          # Заказ собирается
    READY = "ready"              # Заказ готов к выдаче/доставке
    IN_DELIVERY = "in_delivery"  # Заказ в доставке
    DELIVERED = "delivered"      # Заказ доставлен
    COMPLETED = "completed"      # Заказ завершен
    CANCELLED = "cancelled"      # Заказ отменен
    FAILED = "failed"            # Ошибка при обработке заказа

# Типы доставки
class DeliveryType:
    PICKUP = "pickup"            # Самовывоз
    DELIVERY = "delivery"        # Доставка
    EXPRESS = "express"          # Экспресс доставка

# Типы заказов (кто получает заказ)
class OrderType:
    SELF = "self"                # Заказчик получает сам
    OTHER = "other"              # Заказ для другого человека
    CORPORATE = "corporate"      # Корпоративный заказ
    GIFT = "gift"                # Подарок
    PROXY = "proxy"              # Заказ по доверенности

# Типы сущностей для QR-кодов и избранного
class EntityType:
    PRODUCT = "product"          # Товар
    LOCATION = "location"        # Локация/магазин

# Статусы отзывов
class ReviewStatus:
    PENDING = "pending"          # Отзыв на модерации
    VISIBLE = "visible"          # Отзыв опубликован
    HIDDEN = "hidden"            # Отзыв скрыт
    REJECTED = "rejected"        # Отзыв отклонен

# Типы цен для маркетплейса
class PriceType:
    CHATTING = "chatting"        # Цена для чат-бота (основная цена маркетплейса)
    RETAIL = "retail"           # Розничная цена
    WHOLESALE = "wholesale"     # Оптовая цена

# Сортировка товаров
class SortOptions:
    PRICE_ASC = "price"         # По цене (возрастание)
    PRICE_DESC = "price_desc"   # По цене (убывание)
    NAME_ASC = "name"           # По названию (А-Я)
    NAME_DESC = "name_desc"     # По названию (Я-А)
    CREATED_ASC = "created_at"  # По дате создания (старые сначала)
    CREATED_DESC = "created_desc"  # По дате создания (новые сначала)
    RATING_DESC = "rating"      # По рейтингу (высокий сначала)
    DISTANCE_ASC = "distance"   # По расстоянию (близкие сначала)

# Сортировка отзывов
class ReviewSortOptions:
    NEWEST = "newest"           # Новые сначала
    OLDEST = "oldest"           # Старые сначала
    HIGHEST = "highest"         # Высокий рейтинг сначала
    LOWEST = "lowest"           # Низкий рейтинг сначала

# Лимиты и ограничения
class Limits:
    MAX_REVIEW_TEXT_LENGTH = 1000    # Максимальная длина текста отзыва
    MIN_REVIEW_TEXT_LENGTH = 10      # Минимальная длина текста отзыва
    MAX_RATING = 5                   # Максимальный рейтинг
    MIN_RATING = 1                   # Минимальный рейтинг
    MAX_PAGE_SIZE = 100              # Максимальный размер страницы
    MIN_PAGE_SIZE = 1                # Минимальный размер страницы
    DEFAULT_PAGE_SIZE = 20           # Размер страницы по умолчанию
    REVIEW_COOLDOWN_HOURS = 24       # Время между отзывами от одного пользователя (часы)

# UTM метки для аналитики
class UTMSources:
    MOBILE_APP = "mobile_app"        # Мобильное приложение
    WEB_APP = "web_app"              # Веб-приложение
    TELEGRAM_BOT = "telegram_bot"    # Telegram бот
    QR_CODE = "qr_code"              # QR-код
    DIRECT = "direct"                # Прямой переход

# Стратегии распределения заказов
class DistributionStrategy:
    NEAREST_VIABLE_WITH_STOCK = "nearest_viable_with_stock"  # Ближайший с наличием
    ROUND_ROBIN = "round_robin"                              # По очереди
    LOAD_BALANCING = "load_balancing"                        # Балансировка нагрузки
    GEOGRAPHIC = "geographic"                                # Географическое распределение

# Сообщения об ошибках
class ErrorMessages:
    PRODUCT_NOT_FOUND = "Товар не найден или не доступен"
    LOCATION_NOT_FOUND = "Локация не найдена или не доступна"
    QR_CODE_NOT_FOUND = "QR-код не найден или неактивен"
    INVALID_ENTITY_TYPE = "Неизвестный тип сущности"
    INVALID_RATING = "Рейтинг должен быть от 1 до 5"
    REVIEW_TEXT_TOO_SHORT = "Текст отзыва должен содержать минимум 10 символов"
    REVIEW_TEXT_TOO_LONG = "Текст отзыва не должен превышать 1000 символов"
    REVIEW_COOLDOWN = "Можно оставить только один отзыв в сутки"
    ALREADY_IN_FAVORITES = "Элемент уже добавлен в избранное"
    FAVORITE_NOT_FOUND = "Элемент не найден в избранном"
    ORDER_QUEUE_FAILED = "Ошибка при обработке заказа"
    INVALID_ENTITY_TYPE_FAVORITE = "Тип сущности должен быть 'product' или 'location'"
