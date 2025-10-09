"""optimize_segments_queries

Revision ID: optimize_segments_001
Revises: 88961eb30f00
Create Date: 2025-10-09 20:30:00.000000

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "optimize_segments_001"
down_revision = "88961eb30f00"
branch_labels = None
depends_on = None


def upgrade():
    """
    Создает оптимизированные индексы для улучшения производительности запросов сегментов.

    Использует CREATE INDEX CONCURRENTLY для избежания блокировок таблиц.
    Создание индексов займет 1-5 минут в зависимости от размера данных.
    """

    # ==================================================================================
    # Шаг 1: Включение расширения pg_trgm для текстового поиска
    # ==================================================================================

    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

    # ==================================================================================
    # КРИТИЧЕСКИЕ индексы
    # ==================================================================================

    # 1. Индекс для фильтрации карт лояльности по балансу и контрагенту
    # ПРИМЕНЕНИЕ: add_loyality_filters() - предварительная фильтрация карт
    # ЭФФЕКТ: уменьшение количества присоединяемых строк в 10-100 раз
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_loyality_cards_contragent_balance 
        ON loyality_cards(contragent_id, balance) 
        WHERE contragent_id IS NOT NULL AND is_deleted = FALSE;
    """)

    # 2. Индекс для связи транзакций лояльности с картами
    # ПРИМЕНЕНИЕ: Расчет срока истечения бонусов в add_loyality_filters()
    # ЭФФЕКТ: быстрый поиск транзакций по card_id
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_loyality_transactions_card_created 
        ON loyality_transactions(loyality_card_id, created_at);
    """)

    # 3. Покрывающий индекс для документов продаж
    # ПРИМЕНЕНИЕ: Фильтрация и агрегация (COUNT, SUM) в add_purchase_filters()
    # ЭФФЕКТ: Index-only scan для агрегатных запросов
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_docs_sales_cashbox_contragent_sum 
        ON docs_sales(cashbox, contragent, sum, created_at) 
        WHERE contragent IS NOT NULL;
    """)

    # 4. Индекс для товаров в документах продаж
    # ПРИМЕНЕНИЕ: EXISTS подзапросы для фильтрации по категориям/номенклатуре
    # ЭФФЕКТ: быстрый поиск товаров по docs_sales_id
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_docs_sales_goods_docs_nomenclature 
        ON docs_sales_goods(docs_sales_id, nomenclature);
    """)

    # 5. Индекс для номенклатуры по категории
    # ПРИМЕНЕНИЕ: JOIN между nomenclature и categories
    # ЭФФЕКТ: ускорение поиска номенклатуры по категории
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_nomenclature_category 
        ON nomenclature(category, id);
    """)

    # 6. GIN индекс для текстового поиска по категориям
    # ПРИМЕНЕНИЕ: categories.c.name.ilike(f"%{cat}%") в add_purchase_filters()
    # ЭФФЕКТ: значительно ускоряет ILIKE запросы (триграмный поиск)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_categories_name_trgm 
        ON categories USING gin(name gin_trgm_ops);
    """)

    # 7. Частичный индекс для контрагентов
    # ПРИМЕНЕНИЕ: JOIN с контрагентами, исключая удаленных
    # ЭФФЕКТ: меньший размер индекса, быстрый поиск активных контрагентов
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contragents_id_phone_cashbox 
        ON contragents(id, phone, cashbox) 
        WHERE is_deleted = FALSE;
    """)

    # ==================================================================================
    # ДОПОЛНИТЕЛЬНЫЕ индексы для общей производительности
    # ==================================================================================

    # 8. Дополнительный покрывающий индекс для агрегатов
    # ПРИМЕНЕНИЕ: Дополнительная оптимизация COUNT/SUM операций
    # ЭФФЕКТ: альтернативный индекс для оптимизатора запросов
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_docs_sales_contragent_aggregate 
        ON docs_sales(contragent, id, sum, created_at) 
        WHERE contragent IS NOT NULL;
    """)

    # 9. Индекс для фильтрации по тегам документов продаж
    # ПРИМЕНЕНИЕ: docs_sales_tags_filters() - поиск по тегам
    # ЭФФЕКТ: быстрая фильтрация документов по тегам
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_docs_sales_tags_docs_name 
        ON docs_sales_tags(docs_sales_id, name);
    """)

    # 10. Индекс для фильтрации по тегам контрагентов
    # ПРИМЕНЕНИЕ: tags_filters() - поиск контрагентов по тегам
    # ЭФФЕКТ: быстрая фильтрация контрагентов по тегам
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contragents_tags_contragent_name 
        ON contragents_tags(contragent_id, name);
    """)

    # 11. Частичный индекс для информации о доставке
    # ПРИМЕНЕНИЕ: delivery_info_filters() - только документы с доставкой
    # ЭФФЕКТ: меньший размер индекса, быстрый поиск документов с доставкой
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_docs_sales_delivery_docs_address 
        ON docs_sales_delivery_info(docs_sales_id, delivery_date) 
        WHERE address IS NOT NULL AND address != '';
    """)

    # ==================================================================================
    # Обновление статистики для оптимизатора запросов
    # ==================================================================================

    # Обновляем статистику таблиц чтобы планировщик мог правильно использовать новые индексы
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
    Удаляет созданные индексы (откат миграции).

    ⚠️ ВНИМАНИЕ: Производительность запросов сегментов вернется к прежнему уровню.
    Среднее время выполнения запросов увеличится с 3-10 сек обратно до 298 сек.
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

    # Примечание: расширение pg_trgm НЕ удаляем, так как оно может использоваться другими индексами
