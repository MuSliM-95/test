"""optimize_segments_queries_indexes

Revision ID: a1b2c3d4e5f6
Revises: f4d18b4db9a1
Create Date: 2025-10-09 12:00:00.000000

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "f4d18b4db9a1"
branch_labels = None
depends_on = None


def upgrade():
    """
    Создает оптимизированные индексы для улучшения производительности запросов сегментов

    ВАЖНО: Использует CREATE INDEX CONCURRENTLY для избежания блокировок таблиц
    """

    # Включаем расширение pg_trgm для текстового поиска (если еще не включено)
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

    # ==================================================================================
    # КРИТИЧЕСКИЕ индексы
    # ==================================================================================

    # 1. Индекс для фильтрации карт лояльности по балансу и контрагенту
    # Используется в add_loyality_filters для предварительной фильтрации
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_loyality_cards_contragent_balance 
        ON loyality_cards(contragent_id, balance) 
        WHERE contragent_id IS NOT NULL AND is_deleted = FALSE;
    """)

    # 2. Индекс для связи транзакций лояльности с картами
    # Используется для расчета срока истечения бонусов
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_loyality_transactions_card_created 
        ON loyality_transactions(loyality_card_id, created_at);
    """)

    # 3. Покрывающий индекс для документов продаж по контрагенту и кассе
    # Оптимизирует агрегатные запросы (COUNT, SUM)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_docs_sales_cashbox_contragent_sum 
        ON docs_sales(cashbox, contragent, sum, created_at) 
        WHERE contragent IS NOT NULL;
    """)

    # 4. Индекс для товаров в документах продаж
    # Используется в EXISTS подзапросах для фильтрации по категориям/номенклатуре
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_docs_sales_goods_docs_nomenclature 
        ON docs_sales_goods(docs_sales_id, nomenclature);
    """)

    # 5. Индекс для номенклатуры по категории
    # Оптимизирует JOIN между nomenclature и categories
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_nomenclature_category 
        ON nomenclature(category, id);
    """)

    # 6. GIN индекс для текстового поиска по категориям с ILIKE
    # Значительно ускоряет поиск по названиям категорий
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_categories_name_trgm 
        ON categories USING gin(name gin_trgm_ops);
    """)

    # 7. Частичный индекс для контрагентов
    # Исключает удаленных контрагентов из индекса
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contragents_id_phone_cashbox 
        ON contragents(id, phone, cashbox) 
        WHERE is_deleted = FALSE;
    """)

    # ==================================================================================
    # ДОПОЛНИТЕЛЬНЫЕ индексы для общей производительности
    # ==================================================================================

    # 8. Покрывающий индекс для агрегатов по документам продаж
    # Дополнительная оптимизация для COUNT и SUM операций
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_docs_sales_contragent_aggregate 
        ON docs_sales(contragent, id, sum, created_at) 
        WHERE contragent IS NOT NULL;
    """)

    # 9. Индекс для фильтрации по тегам документов продаж
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_docs_sales_tags_docs_name 
        ON docs_sales_tags(docs_sales_id, name);
    """)

    # 10. Индекс для фильтрации по тегам контрагентов
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contragents_tags_contragent_name 
        ON contragents_tags(contragent_id, name);
    """)

    # 11. Частичный индекс для информации о доставке
    # Индексирует только документы с адресом доставки
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_docs_sales_delivery_docs_address 
        ON docs_sales_delivery_info(docs_sales_id, delivery_date) 
        WHERE address IS NOT NULL AND address != '';
    """)

    # Обновляем статистику для оптимизатора запросов
    op.execute("ANALYZE loyality_cards;")
    op.execute("ANALYZE loyality_transactions;")
    op.execute("ANALYZE docs_sales;")
    op.execute("ANALYZE docs_sales_goods;")
    op.execute("ANALYZE nomenclature;")
    op.execute("ANALYZE categories;")
    op.execute("ANALYZE contragents;")
    op.execute("ANALYZE contragents_tags;")
    op.execute("ANALYZE docs_sales_tags;")
    op.execute("ANALYZE docs_sales_delivery_info;")


def downgrade():
    """
    Удаляет созданные индексы (откат миграции)

    ВНИМАНИЕ: Производительность запросов может значительно снизиться
    """

    # Удаляем индексы в обратном порядке
    op.execute(
        "DROP INDEX CONCURRENTLY IF EXISTS idx_docs_sales_delivery_docs_address;"
    )
    op.execute(
        "DROP INDEX CONCURRENTLY IF EXISTS idx_contragents_tags_contragent_name;"
    )
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_docs_sales_tags_docs_name;")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_docs_sales_contragent_aggregate;")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_contragents_id_phone_cashbox;")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_categories_name_trgm;")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_nomenclature_category;")
    op.execute(
        "DROP INDEX CONCURRENTLY IF EXISTS idx_docs_sales_goods_docs_nomenclature;"
    )
    op.execute(
        "DROP INDEX CONCURRENTLY IF EXISTS idx_docs_sales_cashbox_contragent_sum;"
    )
    op.execute(
        "DROP INDEX CONCURRENTLY IF EXISTS idx_loyality_transactions_card_created;"
    )
    op.execute(
        "DROP INDEX CONCURRENTLY IF EXISTS idx_loyality_cards_contragent_balance;"
    )
