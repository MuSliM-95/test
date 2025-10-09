"""optimize_segments_queries_fast

Revision ID: optimize_segments_002
Revises: 88961eb30f00
Create Date: 2025-10-09 21:00:00.000000

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "optimize_segments_002"
down_revision = "88961eb30f00"
branch_labels = None
depends_on = None


def upgrade():
    """
    Создает индексы БЕЗ CONCURRENTLY (быстрее, но блокирует таблицы).
    """

    # Включаем расширение pg_trgm
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # Создаем индексы (БЕЗ CONCURRENTLY = быстрее, но с блокировками)

    # 1. Карты лояльности
    op.create_index(
        "idx_loyality_cards_contragent_balance",
        "loyality_cards",
        ["contragent_id", "balance"],
        postgresql_where="contragent_id IS NOT NULL AND is_deleted = FALSE",
    )

    # 2. Транзакции лояльности
    op.create_index(
        "idx_loyality_transactions_card_created",
        "loyality_transactions",
        ["loyality_card_id", "created_at"],
    )

    # 3. Документы продаж (покрывающий индекс)
    op.create_index(
        "idx_docs_sales_cashbox_contragent_sum",
        "docs_sales",
        ["cashbox", "contragent", "sum", "created_at"],
        postgresql_where="contragent IS NOT NULL",
    )

    # 4. Товары в документах
    op.create_index(
        "idx_docs_sales_goods_docs_nomenclature",
        "docs_sales_goods",
        ["docs_sales_id", "nomenclature"],
    )

    # 5. Номенклатура по категории
    op.create_index("idx_nomenclature_category", "nomenclature", ["category", "id"])

    # 6. GIN индекс для текстового поиска по категориям
    op.execute("""
        CREATE INDEX idx_categories_name_trgm 
        ON categories USING gin(name gin_trgm_ops)
    """)

    # 7. Контрагенты
    op.create_index(
        "idx_contragents_id_phone_cashbox",
        "contragents",
        ["id", "phone", "cashbox"],
        postgresql_where="is_deleted = FALSE",
    )

    # 8. Дополнительный индекс для агрегатов
    op.create_index(
        "idx_docs_sales_contragent_aggregate",
        "docs_sales",
        ["contragent", "id", "sum", "created_at"],
        postgresql_where="contragent IS NOT NULL",
    )

    # 9. Теги документов продаж
    op.create_index(
        "idx_docs_sales_tags_docs_name", "docs_sales_tags", ["docs_sales_id", "name"]
    )

    # 10. Теги контрагентов
    op.create_index(
        "idx_contragents_tags_contragent_name",
        "contragents_tags",
        ["contragent_id", "name"],
    )

    # 11. Информация о доставке
    op.create_index(
        "idx_docs_sales_delivery_docs_address",
        "docs_sales_delivery_info",
        ["docs_sales_id", "delivery_date"],
        postgresql_where="address IS NOT NULL AND address != ''",
    )

    # Обновляем статистику
    op.execute("ANALYZE loyality_cards")
    op.execute("ANALYZE loyality_transactions")
    op.execute("ANALYZE docs_sales")
    op.execute("ANALYZE docs_sales_goods")
    op.execute("ANALYZE nomenclature")
    op.execute("ANALYZE categories")
    op.execute("ANALYZE contragents")


def downgrade():
    """Удаляет созданные индексы"""

    op.drop_index("idx_docs_sales_delivery_docs_address", "docs_sales_delivery_info")
    op.drop_index("idx_contragents_tags_contragent_name", "contragents_tags")
    op.drop_index("idx_docs_sales_tags_docs_name", "docs_sales_tags")
    op.drop_index("idx_docs_sales_contragent_aggregate", "docs_sales")
    op.drop_index("idx_contragents_id_phone_cashbox", "contragents")
    op.drop_index("idx_categories_name_trgm", "categories")
    op.drop_index("idx_nomenclature_category", "nomenclature")
    op.drop_index("idx_docs_sales_goods_docs_nomenclature", "docs_sales_goods")
    op.drop_index("idx_docs_sales_cashbox_contragent_sum", "docs_sales")
    op.drop_index("idx_loyality_transactions_card_created", "loyality_transactions")
    op.drop_index("idx_loyality_cards_contragent_balance", "loyality_cards")
