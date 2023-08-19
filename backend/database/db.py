import databases
import sqlalchemy
from sqlalchemy import Boolean, ForeignKey, Integer, String, Text, Float, DateTime, Date, JSON, BigInteger, UniqueConstraint, ARRAY, Enum
from sqlalchemy.sql import func
import os
from enum import Enum as ENUM


class CostType(str, ENUM):
    per_user = "per_user"
    per_account = "per_account"


class Trial(ENUM):
    secon="secon"
    link: str


metadata = sqlalchemy.MetaData()

entity_type = sqlalchemy.Table(
    "entity_type",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("code", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

tasks = sqlalchemy.Table(
    "tasks",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("description", Text),
    sqlalchemy.Column("report", String),
    sqlalchemy.Column("integration_id", ForeignKey("integrations.id")),
    sqlalchemy.Column("status", String),
    sqlalchemy.Column("creator", Integer), # 0 - robot
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

jwt_scopes = sqlalchemy.Table(
    "jwt_scopes",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("interaction", String, nullable=False),
    sqlalchemy.Column("scope", String, nullable=False)
)

integrations_type = sqlalchemy.Table(
    "integrations_type",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

integrations_to_cashbox = sqlalchemy.Table(
    "integrations_to_cashbox",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("integration_id", ForeignKey("integrations.id"), nullable=False),
    sqlalchemy.Column("installed_by", ForeignKey("relation_tg_cashboxes.id"), nullable=False),
    sqlalchemy.Column("deactivated_by", ForeignKey("relation_tg_cashboxes.id"), nullable=False),
    sqlalchemy.Column("status", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

integrations = sqlalchemy.Table(
    "integrations",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("status", Boolean, nullable=False),
    sqlalchemy.Column("integrations_type", ForeignKey("integrations_type.id")),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("description_short", String),
    sqlalchemy.Column("description_long", Text),
    sqlalchemy.Column("folder_name", String),
    sqlalchemy.Column("microservice_id", Integer),
    sqlalchemy.Column("image", String),
    sqlalchemy.Column("images", ARRAY(item_type=String)),
    sqlalchemy.Column("owner", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("is_public", Boolean),
    sqlalchemy.Column("cost", Integer),
    sqlalchemy.Column("cost_type", String),
    sqlalchemy.Column("cost_percent", Float),
    sqlalchemy.Column("payed_to", Date),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
    sqlalchemy.Column("is_payed", Boolean),
    sqlalchemy.Column("trial", String),
    sqlalchemy.Column("client_secret", String),
    sqlalchemy.Column("code", String),
    sqlalchemy.Column("scopes", Text),
    sqlalchemy.Column("redirect_uri", String),
    sqlalchemy.Column("url", String)
)

cboxes = sqlalchemy.Table(
    "cashboxes",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("balance", Float),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("admin", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("invite_token", String, unique=True),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

organizations = sqlalchemy.Table(
    "organizations",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("type", String, nullable=False),
    sqlalchemy.Column("short_name", String, nullable=False),
    sqlalchemy.Column("full_name", String),
    sqlalchemy.Column("work_name", String),
    sqlalchemy.Column("prefix", String),
    sqlalchemy.Column("inn", BigInteger),
    sqlalchemy.Column("kpp", BigInteger),
    sqlalchemy.Column("okved", BigInteger),
    sqlalchemy.Column("okved2", BigInteger),
    sqlalchemy.Column("okpo", BigInteger),
    sqlalchemy.Column("ogrn", BigInteger),
    sqlalchemy.Column("registration_date", Integer),
    sqlalchemy.Column("org_type", String),
    sqlalchemy.Column("tax_type", String),
    sqlalchemy.Column("tax_percent", Float),
    sqlalchemy.Column("owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

contracts = sqlalchemy.Table(
    "contracts",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("number", String, nullable=False),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("print_name", String),
    sqlalchemy.Column("dated", Integer),
    sqlalchemy.Column("used_from", Integer),
    sqlalchemy.Column("used_to", Integer),
    sqlalchemy.Column("status", Boolean, nullable=False),
    sqlalchemy.Column("contragent", Integer, ForeignKey("contragents.id")),
    sqlalchemy.Column("organization", Integer, ForeignKey("organizations.id")),
    sqlalchemy.Column("payment_type", String),
    sqlalchemy.Column("payment_time", String),
    sqlalchemy.Column("owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

nomenclature = sqlalchemy.Table(
    "nomenclature",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("type", String),
    sqlalchemy.Column("description_short", String),
    sqlalchemy.Column("description_long", String),
    sqlalchemy.Column("code", Integer),
    sqlalchemy.Column("unit", Integer, ForeignKey("units.id")),
    sqlalchemy.Column("category", Integer, ForeignKey("categories.id")),
    sqlalchemy.Column("manufacturer", Integer, ForeignKey("manufacturers.id")),
    sqlalchemy.Column("owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

categories = sqlalchemy.Table(
    "categories",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("description", String),
    sqlalchemy.Column("code", Integer),
    sqlalchemy.Column("parent", Integer, ForeignKey("categories.id")),
    sqlalchemy.Column("owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("status", Boolean, nullable=False),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

units = sqlalchemy.Table(
    "units",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("code", Integer),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("convent_national_view", String),
    sqlalchemy.Column("convent_international_view", String),
    sqlalchemy.Column("symbol_national_view", String),
    sqlalchemy.Column("symbol_international_view", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
)

warehouses = sqlalchemy.Table(
    "warehouses",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("type", String),
    sqlalchemy.Column("description", String),
    sqlalchemy.Column("address", String),
    sqlalchemy.Column("phone", String),
    sqlalchemy.Column("parent", Integer, ForeignKey("warehouses.id")),
    sqlalchemy.Column("owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("status", Boolean, nullable=False),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

manufacturers = sqlalchemy.Table(
    "manufacturers",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

prices = sqlalchemy.Table(
    "prices",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("price_type", Integer, ForeignKey("price_types.id")),
    sqlalchemy.Column("price", Float, nullable=False),
    sqlalchemy.Column("nomenclature", Integer, ForeignKey("nomenclature.id"), nullable=False),
    sqlalchemy.Column("date_from", Integer),
    sqlalchemy.Column("date_to", Integer),
    sqlalchemy.Column("owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

price_types = sqlalchemy.Table(
    "price_types",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

pictures = sqlalchemy.Table(
    "pictures",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("entity", String, nullable=False),
    sqlalchemy.Column("entity_id", Integer, nullable=False),
    sqlalchemy.Column("url", String, nullable=False),
    sqlalchemy.Column("size", Integer),
    sqlalchemy.Column("is_main", Boolean),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    ),
)

entity_or_function = sqlalchemy.Table(
    "entity_or_function",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, nullable=False, unique=True),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    ),
)

status_entity_function = sqlalchemy.Table(
    "status_entity_function",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "entity_or_function",
        String,
        ForeignKey("entity_or_function.name", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=False
    ),
    sqlalchemy.Column("status", Boolean, nullable=False),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    ),
    UniqueConstraint("entity_or_function", "cashbox", name="function_cashbox_unique")
)

pboxes = sqlalchemy.Table(
    "payboxes",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, default="default"),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("start_balance", Float, default=0),
    sqlalchemy.Column("balance", Float, default=0),
    sqlalchemy.Column("balance_date", Integer),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("update_start_balance", Integer),
    sqlalchemy.Column("update_start_balance_date", Integer),
    sqlalchemy.Column("organization_id", Integer, ForeignKey("organizations.id")),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

users = sqlalchemy.Table(
    "tg_accounts",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("is_admin", Boolean, default=False),
    sqlalchemy.Column("is_blocked", Boolean, default=False),
    sqlalchemy.Column("chat_id", String, unique=True),
    sqlalchemy.Column("owner_id", String),
    sqlalchemy.Column("phone_number", String),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("photo", String),
    sqlalchemy.Column("first_name", String),
    sqlalchemy.Column("last_name", String),
    sqlalchemy.Column("username", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

events = sqlalchemy.Table(
    "events",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("type", String),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("method", String),
    sqlalchemy.Column("url", String),
    sqlalchemy.Column("payload", JSON),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("user_id", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("token", String),
    sqlalchemy.Column("ip", String),
    sqlalchemy.Column("promoimage", String),
    sqlalchemy.Column("promodata", JSON),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), onupdate=func.now()),
)

installs = sqlalchemy.Table(
    "installs",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("iosversion", String),
    sqlalchemy.Column("phone", String),
    sqlalchemy.Column("devicetoken", String, unique=True),
    sqlalchemy.Column("md5key", String),
    sqlalchemy.Column("geolocation", String),
    sqlalchemy.Column("push", Boolean),
    sqlalchemy.Column("my_push", String),
    sqlalchemy.Column("foreign_push", String),
    sqlalchemy.Column("contacts", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), onupdate=func.now()),
)

links = sqlalchemy.Table(
    "links",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("user_id", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("install_id", Integer, ForeignKey("installs.id"), unique=True),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("tg_token", String, unique=True),
    sqlalchemy.Column("delinked", Boolean, default=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), onupdate=func.now()),
)

users_cboxes_relation = sqlalchemy.Table(
    "relation_tg_cashboxes",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("user", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("token", String),
    sqlalchemy.Column("status", Boolean, default=True),
    sqlalchemy.Column("is_owner", Boolean, default=True),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

contragents = sqlalchemy.Table(
    "contragents",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("phone", String, nullable=True),
    sqlalchemy.Column("inn", String, nullable=True),
    sqlalchemy.Column("description", Text),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),

    sqlalchemy.Column("is_deleted", Boolean),

    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

articles = sqlalchemy.Table(
    "articles",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("icon_file", String),
    sqlalchemy.Column("emoji", String),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("code", Integer),
    sqlalchemy.Column("description", String),
    sqlalchemy.Column("expenses_for", String),
    sqlalchemy.Column("distribute_according", String),
    sqlalchemy.Column("distribute_for", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

payments = sqlalchemy.Table(
    "payments",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("type", String),
    sqlalchemy.Column("name", String, nullable=True),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("article", String, nullable=True),
    sqlalchemy.Column("project_id", Integer, ForeignKey("projects.id"), nullable=True),
    sqlalchemy.Column("article_id", Integer, ForeignKey("articles.id"), nullable=True),
    sqlalchemy.Column("tags", String),
    sqlalchemy.Column("amount", Float),
    sqlalchemy.Column("amount_without_tax", Float),
    sqlalchemy.Column("description", Text, nullable=True),
    sqlalchemy.Column("date", Integer),
    sqlalchemy.Column("repeat_freq", Integer, nullable=True),
    sqlalchemy.Column("parent_id", Integer, nullable=True),
    sqlalchemy.Column("repeat_parent_id", Integer, ForeignKey("payments.id"), nullable=True),
    sqlalchemy.Column("repeat_period", String, nullable=True),
    sqlalchemy.Column("repeat_first", Integer, nullable=True),
    sqlalchemy.Column("repeat_last", Integer, nullable=True),
    sqlalchemy.Column("repeat_number", Integer, nullable=True),
    sqlalchemy.Column("repeat_day", Integer, nullable=True),
    sqlalchemy.Column("repeat_month", Integer, nullable=True),
    sqlalchemy.Column("repeat_seconds", Integer, nullable=True),
    sqlalchemy.Column("repeat_weekday", String, nullable=True),
    sqlalchemy.Column("stopped", Boolean, default=False),
    sqlalchemy.Column("status", Boolean, default=True),
    sqlalchemy.Column("tax", Float, nullable=True),
    sqlalchemy.Column("tax_type", String, nullable=True),
    sqlalchemy.Column("deb_cred", Boolean, default=False),
    sqlalchemy.Column("raspilen", Boolean, default=False),
    sqlalchemy.Column("contragent", Integer, ForeignKey("contragents.id"), nullable=True),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("paybox", Integer, ForeignKey("payboxes.id")),
    sqlalchemy.Column("paybox_to", Integer, ForeignKey("payboxes.id"), nullable=True),
    sqlalchemy.Column("account", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("is_deleted", Boolean, default=False),
    sqlalchemy.Column("cheque", Integer, ForeignKey("cheques.id"), nullable=True),
    sqlalchemy.Column("docs_sales_id", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("contract_id", Integer, ForeignKey("contracts.id")),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)


projects = sqlalchemy.Table(
    "projects",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("incoming", Float),
    sqlalchemy.Column("outgoing", Float),
    sqlalchemy.Column("profitability", Float),
    sqlalchemy.Column("proj_sum", Float),
    sqlalchemy.Column("icon_file", String),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

tariffs = sqlalchemy.Table(
    "pay_tariffs",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, unique=True),
    sqlalchemy.Column("price", Float, nullable=False),
    sqlalchemy.Column("per_user", Boolean, default=False, nullable=False),
    sqlalchemy.Column("frequency", Integer, default=30, nullable=False),
    sqlalchemy.Column("archived", Boolean, default=False, nullable=False),
    sqlalchemy.Column("actual", Boolean, default=True, nullable=False),
    sqlalchemy.Column("demo_days", Integer, nullable=False),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

transactions = sqlalchemy.Table(
    "pay_transactions",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("tariff", Integer, ForeignKey("pay_tariffs.id")),
    sqlalchemy.Column("users", Integer),
    sqlalchemy.Column("amount", Float),
    sqlalchemy.Column("status", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

accounts_balances = sqlalchemy.Table(
    "pay_accounts_balances",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("tariff", Integer, ForeignKey("pay_tariffs.id"), nullable=False),
    sqlalchemy.Column("last_transaction", Integer, ForeignKey("pay_transactions.id"), nullable=True),
    sqlalchemy.Column("balance", Float),
    sqlalchemy.Column("tariff_type", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

cheques = sqlalchemy.Table(
    "cheques",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("user", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("data", JSON),
    sqlalchemy.Column("created_at", Integer),
)

amo_install = sqlalchemy.Table(
    "amo_install",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("code", String),
    sqlalchemy.Column("last_code", Integer),
    sqlalchemy.Column("referrer", String),
    sqlalchemy.Column("platform", Integer),
    sqlalchemy.Column("amo_account_id", Integer),
    sqlalchemy.Column("client_id", String),
    sqlalchemy.Column("client_secret", String),
    sqlalchemy.Column("from_widget", Integer),
    sqlalchemy.Column("refresh_token", String),
    sqlalchemy.Column("access_token", String),
    sqlalchemy.Column("pair_token", String),
    sqlalchemy.Column("expires_in", Integer),
    sqlalchemy.Column("active", Boolean),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

amo_integrations = sqlalchemy.Table(
    "amo_integrations",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("type", String),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

amo_install_table_cashboxes = sqlalchemy.Table(
    "amo_install_table_cashboxes",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("amo_integration_id", Integer, ForeignKey("amo_install.id")),
    sqlalchemy.Column("last_token", String),
    sqlalchemy.Column("status", Boolean),
    sqlalchemy.Column("additional_info", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

amo_events_type = sqlalchemy.Table(
    "amo_events_type",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("event_id", Integer, ForeignKey("events.id")),
    sqlalchemy.Column("type", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

custom_fields = sqlalchemy.Table(
    "custom_fields",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("entity", String),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("type", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

custom_fields_values = sqlalchemy.Table(
    "custom_fields_values",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cf_id", Integer, ForeignKey("custom_fields.id")),
    sqlalchemy.Column("value", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

docs_sales = sqlalchemy.Table(
    "docs_sales",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("number", String),
    sqlalchemy.Column("dated", Integer),
    sqlalchemy.Column("operation", String),
    sqlalchemy.Column("tags", String),
    sqlalchemy.Column("docs_sales", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("comment", String),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("contragent", Integer, ForeignKey("contragents.id")),
    sqlalchemy.Column("contract", Integer, ForeignKey("contracts.id")),
    sqlalchemy.Column("organization", Integer, ForeignKey("organizations.id"), nullable=False),
    sqlalchemy.Column("warehouse", Integer, ForeignKey("warehouses.id")),
    sqlalchemy.Column("status", Boolean),
    sqlalchemy.Column("tax_included", Boolean),
    sqlalchemy.Column("tax_active", Boolean),
    sqlalchemy.Column("sales_manager", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("sum", Float),
    sqlalchemy.Column("created_by", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

docs_sales_goods = sqlalchemy.Table(
    "docs_sales_goods",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("docs_sales_id", Integer, ForeignKey("docs_sales.id"), nullable=False),
    sqlalchemy.Column("nomenclature", Integer, ForeignKey("nomenclature.id"), nullable=False),
    sqlalchemy.Column("price_type", Integer, ForeignKey("price_types.id")),
    sqlalchemy.Column("price", Float, nullable=False),
    sqlalchemy.Column("quantity", Integer, nullable=False),
    sqlalchemy.Column("unit", Integer, ForeignKey("units.id")),
    sqlalchemy.Column("tax", Float),
    sqlalchemy.Column("discount", Float),
    sqlalchemy.Column("sum_discounted", Float),
    sqlalchemy.Column("status", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

docs_purchases = sqlalchemy.Table(
    "docs_purchases",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("number", String),
    sqlalchemy.Column("dated", Integer),
    sqlalchemy.Column("operation", String),
    sqlalchemy.Column("comment", String),
    sqlalchemy.Column("client", Integer, ForeignKey("contragents.id")),
    sqlalchemy.Column("contragent", Integer, ForeignKey("contragents.id")),
    sqlalchemy.Column("contract", Integer, ForeignKey("contracts.id")),
    sqlalchemy.Column("organization", Integer, ForeignKey("organizations.id"), nullable=False),
    sqlalchemy.Column("warehouse", Integer, ForeignKey("warehouses.id")),
    sqlalchemy.Column("purchased_by", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("sum", Float),
    sqlalchemy.Column("created_by", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("docs_sales_id", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

docs_purchases_goods = sqlalchemy.Table(
    "docs_purchases_goods",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("docs_purchases_id", Integer, ForeignKey("docs_purchases.id"), nullable=False),
    sqlalchemy.Column("nomenclature", Integer, ForeignKey("nomenclature.id"), nullable=False),
    sqlalchemy.Column("price_type", Integer, ForeignKey("price_types.id")),
    sqlalchemy.Column("price", Float, nullable=False),
    sqlalchemy.Column("quantity", Integer, nullable=False),
    sqlalchemy.Column("unit", Integer, ForeignKey("units.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

docs_warehouse = sqlalchemy.Table(
    "docs_warehouse",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("number", String),
    sqlalchemy.Column("dated", Integer),
    sqlalchemy.Column("operation", String),
    sqlalchemy.Column("comment", String),
    sqlalchemy.Column("organization", Integer, ForeignKey("organizations.id"), nullable=False),
    sqlalchemy.Column("docs_sales_id", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("warehouse", Integer, ForeignKey("warehouses.id")),
    sqlalchemy.Column("sum", Float),
    sqlalchemy.Column("created_by", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

docs_warehouse_goods = sqlalchemy.Table(
    "docs_warehouse_goods",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("docs_warehouse_id", Integer, ForeignKey("docs_warehouse.id"), nullable=False),
    sqlalchemy.Column("nomenclature", Integer, ForeignKey("nomenclature.id"), nullable=False),
    sqlalchemy.Column("price_type", Integer, ForeignKey("price_types.id")),
    sqlalchemy.Column("price", Float, nullable=False),
    sqlalchemy.Column("quantity", Integer, nullable=False),
    sqlalchemy.Column("unit", Integer, ForeignKey("units.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

docs_reconciliation = sqlalchemy.Table(
    "docs_reconciliation",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("number", String),
    sqlalchemy.Column("dated", Integer),
    sqlalchemy.Column("organization", Integer, ForeignKey("organizations.id"), nullable=False),
    sqlalchemy.Column("contragent", Integer, ForeignKey("contragents.id"), nullable=False),
    sqlalchemy.Column("contract", Integer, ForeignKey("contracts.id")),
    sqlalchemy.Column("organization_name", String),
    sqlalchemy.Column("contragent_name", String),
    sqlalchemy.Column("period_from", Integer),
    sqlalchemy.Column("period_to", Integer),
    sqlalchemy.Column("documents", JSON),
    sqlalchemy.Column("documents_grouped", JSON),
    sqlalchemy.Column("organization_period_debt", Float),
    sqlalchemy.Column("organization_period_credit", Float),
    sqlalchemy.Column("contragent_period_debt", Float),
    sqlalchemy.Column("contragent_period_credit", Float),
    sqlalchemy.Column("organization_initial_balance", Float),
    sqlalchemy.Column("contragent_initial_balance", Float),
    sqlalchemy.Column("organization_closing_balance", Float),
    sqlalchemy.Column("contragent_closing_balance", Float),
    sqlalchemy.Column("created_by", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

entity_to_entity = sqlalchemy.Table(
    "entity_to_entity",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("type", String, nullable=False),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False),
    sqlalchemy.Column("from_entity", Integer, ForeignKey("entity_or_function.id"), nullable=False),
    sqlalchemy.Column("from_id", Integer, nullable=False),
    sqlalchemy.Column("to_entity", Integer, ForeignKey("entity_or_function.id"), nullable=False),
    sqlalchemy.Column("to_id", Integer, nullable=False),
    sqlalchemy.Column("status", Boolean, default=sqlalchemy.true),
    sqlalchemy.Column("delinked", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

distribution_docs = sqlalchemy.Table(
    "distribution_docs",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("organization", Integer, ForeignKey("organizations.id")),
    sqlalchemy.Column("period_start", Integer, nullable=False),
    sqlalchemy.Column("period_end", Integer, nullable=False),
    sqlalchemy.Column("is_preview", Boolean),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

distribution_docs_operations = sqlalchemy.Table(
    "distribution_fifo_operations",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("distribution_fifo", Integer, ForeignKey("distribution_docs.id", ondelete="CASCADE")),
    sqlalchemy.Column("document_sale", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("document_purchase", Integer, ForeignKey("docs_purchases.id")),
    sqlalchemy.Column("document_warehouse", Integer, ForeignKey("docs_warehouse.id")),
    sqlalchemy.Column("nomenclature", Integer, ForeignKey("nomenclature.id")),
    sqlalchemy.Column("dated", Integer, nullable=False),
    sqlalchemy.Column("start_amount", Integer, nullable=False),
    sqlalchemy.Column("start_price", Float, nullable=False),
    sqlalchemy.Column("incoming_amount", Integer),
    sqlalchemy.Column("incoming_price", Float),
    sqlalchemy.Column("outgoing_amount", Integer),
    sqlalchemy.Column("outgoing_price", Float),
    sqlalchemy.Column("end_amount", Integer, nullable=False),
    sqlalchemy.Column("end_price", Float, nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

gross_profit_docs = sqlalchemy.Table(
    "gross_profit_docs",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("organization", Integer, ForeignKey("organizations.id")),
    sqlalchemy.Column("period_start", Integer, nullable=False),
    sqlalchemy.Column("period_end", Integer, nullable=False),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

gross_profit_docs_operations = sqlalchemy.Table(
    "gross_profit_docs_operations",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("gross_profit_doc_id", Integer, ForeignKey("gross_profit_docs.id", ondelete="CASCADE"), nullable=False),
    sqlalchemy.Column("document_sale", Integer, ForeignKey("docs_sales.id"), nullable=False),
    sqlalchemy.Column("net_cost", Float, nullable=False),
    sqlalchemy.Column("sum", Float, nullable=False),
    sqlalchemy.Column("actual_revenue", Float, nullable=False),
    sqlalchemy.Column("direct_costs", Float, nullable=False),
    sqlalchemy.Column("indirect_costs", Float, nullable=False),
    sqlalchemy.Column("gross_profit", Float, nullable=False),
    sqlalchemy.Column("rentability", Float, nullable=False),
    sqlalchemy.Column("sales_manager", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

fifo_settings = sqlalchemy.Table(
    "fifo_settings",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("fully_closed_date", Integer, nullable=False),
    sqlalchemy.Column("temporary_closed_date", Integer),
    sqlalchemy.Column("blocked_date", Integer),
    sqlalchemy.Column("month_closing_delay_days", Integer),
    sqlalchemy.Column("preview_close_period_seconds", Integer, nullable=False),
    sqlalchemy.Column("organization_id", Integer, ForeignKey("organizations.id"), nullable=False, unique=True),
    sqlalchemy.Column("in_progress", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)


warehouse_balances = sqlalchemy.Table(
    "warehouse_balances",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("organization_id", Integer, ForeignKey("organizations.id")),
    sqlalchemy.Column("warehouse_id", Integer, ForeignKey("warehouses.id")),
    sqlalchemy.Column("nomenclature_id", Integer, ForeignKey("nomenclature.id")),
    sqlalchemy.Column("document_sale_id", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("document_purchase_id", Integer, ForeignKey("docs_purchases.id")),
    sqlalchemy.Column("document_warehouse_id", Integer, ForeignKey("docs_warehouse.id")),
    sqlalchemy.Column("incoming_amount", Integer),
    sqlalchemy.Column("outgoing_amount", Integer),
    sqlalchemy.Column("current_amount", Integer, nullable=False),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)


messages = sqlalchemy.Table(
    "messages",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("tg_message_id", Integer),
    sqlalchemy.Column("tg_user_or_chat", String),
    sqlalchemy.Column("body", String),
    sqlalchemy.Column("from_or_to", String),
    sqlalchemy.Column("created_at", String),
    sqlalchemy.Column("updated_at", String),
)

loyality_cards = sqlalchemy.Table(
    "loyality_cards",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("card_number", BigInteger),
    sqlalchemy.Column("tags", String),
    sqlalchemy.Column("balance", Integer),
    sqlalchemy.Column("income", Integer),
    sqlalchemy.Column("outcome", Integer),
    sqlalchemy.Column("cashback_percent", Integer),
    sqlalchemy.Column("minimal_checque_amount", Integer),
    sqlalchemy.Column("start_period", DateTime),
    sqlalchemy.Column("end_period", DateTime),
    sqlalchemy.Column("max_percentage", Integer),
    sqlalchemy.Column("max_withdraw_percentage", Integer),
    sqlalchemy.Column("contragent_id", ForeignKey("contragents.id")),
    sqlalchemy.Column("organization_id", ForeignKey("organizations.id"), default=1),
    sqlalchemy.Column("cashbox_id", ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_by_id", ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("status_card", Boolean),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
)

loyality_transactions = sqlalchemy.Table(
    "loyality_transactions",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("type", String, default="accrual"),
    sqlalchemy.Column("dated", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("amount", Float),
    sqlalchemy.Column("loyality_card_id", ForeignKey("loyality_cards.id")),
    sqlalchemy.Column("loyality_card_number", Integer, index=True),
    sqlalchemy.Column("created_by_id", ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("docs_sales_id", ForeignKey("docs_sales.id")),
    sqlalchemy.Column("cashbox", ForeignKey("cashboxes.id")),
    sqlalchemy.Column("tags", String),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("description", String),
    sqlalchemy.Column("status", Boolean, default=True),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("cashier_name", String),
    sqlalchemy.Column("dead_at", DateTime),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
)

loyality_settings = sqlalchemy.Table(
    "loyality_settings",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("organization", Integer, ForeignKey("organizations.id")),
    sqlalchemy.Column("tags", String),
    sqlalchemy.Column("cashback_percent", Integer),
    sqlalchemy.Column("minimal_checque_amount", Integer),
    sqlalchemy.Column("start_period", DateTime),
    sqlalchemy.Column("end_period", DateTime),
    sqlalchemy.Column("max_withdraw_percentage", Integer),
    sqlalchemy.Column("max_percentage", Integer),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
)

SQLALCHEMY_DATABASE_URL = f"postgresql://{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASS')}@db/cash_2"
database = databases.Database(SQLALCHEMY_DATABASE_URL)
engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URL)
