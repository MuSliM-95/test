import json
import os
import time
import logging
from fastapi import FastAPI, Request, Query
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from api.categories.web.InstallCategoriesWeb import InstallCategoriesWeb
from api.contragents.web.InstallContragentsWeb import InstallContragentsWeb
from api.docs_sales.web.InstallDocsSalesWeb import InstallDocsSalesWeb
from api.loyality_transactions.web.InstallLoyalityTransactionsWeb import InstallLoyalityTransactionsWeb
from api.manufacturers.web.InstallManufacturersWeb import InstallManufacturersWeb
from api.nomenclature.infrastructure.readers.core.INomenclatureReader import INomenclatureReader
from api.nomenclature.infrastructure.readers.impl.NomenclatureReader import NomenclatureReader
from api.nomenclature.web.InstallNomenclatureWeb import InstallNomenclatureWeb
from api.nomenclature_attributes.infrastructure.functions.core.IDeleteNomenclatureAttributesFunction import \
    IDeleteNomenclatureAttributesFunction
from api.nomenclature_attributes.infrastructure.functions.core.IInsertNomenclatureAttributesFunction import \
    IInsertNomenclatureAttributesFunction
from api.nomenclature_attributes.infrastructure.functions.impl.DeleteNomenclatureAttributesFunction import \
    DeleteNomenclatureAttributesFunction
from api.nomenclature_attributes.infrastructure.functions.impl.InsertNomenclatureAttributesFunction import \
    InsertNomenclatureAttributesFunction
from api.nomenclature_attributes.infrastructure.readers.core.INomenclatureAttributesReader import \
    INomenclatureAttributesReader
from api.nomenclature_attributes.infrastructure.readers.impl.NomenclatureAttributesReader import \
    NomenclatureAttributesReader
from api.nomenclature_attributes.web.InstallNomenclatureAttributesWeb import InstallNomenclatureAttributesWeb
from api.nomenclature_groups.infrastructure.functions.core.IAddNomenclatureToGroupFunction import \
    IAddNomenclatureToGroupFunction
from api.nomenclature_groups.infrastructure.functions.core.IChangeMainNomenclGroupFunction import \
    IChangeMainNomenclGroupFunction
from api.nomenclature_groups.infrastructure.functions.core.ICreateNomenclatureGroupFunction import \
    ICreateNomenclatureGroupFunction
from api.nomenclature_groups.infrastructure.functions.core.IDelNomenclatureFromGroupFunction import \
    IDelNomenclatureFromGroupFunction
from api.nomenclature_groups.infrastructure.functions.core.IDeleteNomenclatureGroupFunction import \
    IDeleteNomenclatureGroupFunction
from api.nomenclature_groups.infrastructure.functions.core.IPatchNomenclatureGroupFunction import \
    IPatchNomenclatureGroupFunction
from api.nomenclature_groups.infrastructure.functions.impl.AddNomenclatureToGroupFunction import \
    AddNomenclatureToGroupFunction
from api.nomenclature_groups.infrastructure.functions.impl.ChangeMainNomenclGroupFunction import \
    ChangeMainNomenclGroupFunction
from api.nomenclature_groups.infrastructure.functions.impl.CreateNomenclatureGroupFunction import \
    CreateNomenclatureGroupFunction
from api.nomenclature_groups.infrastructure.functions.impl.DelNomenclatureFromGroupFunction import \
    DelNomenclatureFromGroupFunction
from api.nomenclature_groups.infrastructure.functions.impl.DeleteNomenclatureGroupFunction import \
    DeleteNomenclatureGroupFunction
from api.nomenclature_groups.infrastructure.functions.impl.PatchNomenclatureGroupFunction import \
    PatchNomenclatureGroupFunction
from api.nomenclature_groups.infrastructure.readers.core.INomenclatureGroupsReader import INomenclatureGroupsReader
from api.nomenclature_groups.infrastructure.readers.impl.NomenclatureGroupsReader import NomenclatureGroupsReader
from api.nomenclature_groups.web.InstallNomenclatureGroupsWeb import InstallNomenclatureGroupsWeb
from apps.amocrm.installer.infrastructure.repositories.core.IWidgetInstallerRepository import \
    IWidgetInstallerRepository
from apps.amocrm.installer.infrastructure.repositories.impl.WidgetInstallerRepository import \
    WidgetInstallerRepository
from apps.amocrm.installer.web.InstallWidgetInstallerInfoWeb import InstallWidgetInstallerInfoWeb
from apps.booking.booking.infrastructure.repositories.core.IBookingRepository import IBookingRepository
from apps.booking.booking.infrastructure.repositories.impl.BookingRepository import BookingRepository
from apps.booking.events.infrastructure.repositories.core.IBookingEventsRepository import IBookingEventsRepository
from apps.booking.events.infrastructure.repositories.impl.BookingEventsRepository import BookingEventsRepository
from apps.booking.events.web.InstallBookingEventsWeb import InstallBookingEventsWeb
from apps.booking.nomenclature.infrastructure.repositories.core.IBookingNomenclatureRepository import \
    IBookingNomenclatureRepository
from apps.booking.nomenclature.infrastructure.repositories.impl.BookingNomenclatureRepository import \
    BookingNomenclatureRepository
from apps.booking.repeat.web.InstallBookingRepeatWeb import InstallBookingRepeatWeb
from apps.yookassa.repositories.core.IYookassaCrmPaymentsRepository import IYookassaCrmPaymentsRepository
from apps.yookassa.repositories.core.IYookassaOauthRepository import IYookassaOauthRepository
from apps.yookassa.repositories.core.IYookassaPaymentsRepository import IYookassaPaymentsRepository
from apps.yookassa.repositories.core.IYookassaRequestRepository import IYookassaRequestRepository
from apps.yookassa.repositories.core.IYookassaTableNomenclature import IYookassaTableNomenclature
from apps.yookassa.repositories.core.IYookasssaAmoTableCrmRepository import IYookasssaAmoTableCrmRepository
from apps.yookassa.repositories.impl.YookassaCrmPaymentsRepository import YookassaCrmPaymentsRepository
from apps.yookassa.repositories.impl.YookassaOauthRepository import YookassaOauthRepository
from apps.yookassa.repositories.impl.YookassaPaymentsRepository import YookassaPaymentsRepository
from apps.yookassa.repositories.impl.YookassaRequestRepository import YookassaRequestRepository
from apps.yookassa.repositories.impl.YookassaTableNomenclature import YookassaTableNomenclature
from apps.yookassa.repositories.impl.YookasssaAmoTableCrmRepository import YookasssaAmoTableCrmRepository
from apps.yookassa.web.InstallOauthWeb import InstallYookassaOauthWeb
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.impl.RabbitFactory import RabbitFactory
from common.amqp_messaging.models.RabbitMqSettings import RabbitMqSettings
from common.s3_service.core.IS3ServiceFactory import IS3ServiceFactory
from common.s3_service.impl.S3ServiceFactory import S3ServiceFactory
from common.s3_service.models.S3SettingsModel import S3SettingsModel
from common.utils.ioc.ioc import ioc

from database.db import database
from database.fixtures import init_db
# import sentry_sdk

from functions.users import get_user_id_cashbox_id_by_token
from functions.events import write_event

from starlette.types import Message

from api.cashboxes.routers import router as cboxes_router
from api.contragents.routers import router as contragents_router
from api.payments.routers import create_payment, router as payments_router
from api.balances.transactions_routers import router as transactions_router, tinkoff_router
from api.pboxes.routers import router as pboxes_router
from api.projects.routers import router as projects_router
from api.users.routers import router as users_router
from api.websockets.routers import router as websockets_router
from api.articles.routers import router as articles_router
from api.analytics.routers import router as analytics_router
from api.installs.routers import router as installs_router
from api.balances.routers import router as balances_router
from api.cheques.routers import router as cheques_router
from api.events.routers import router as events_router
from api.organizations.routers import router as organizations_router
from api.contracts.routers import router as contracts_router
from api.categories.routers import router as categories_router
from api.warehouses.routers import router as warehouses_router
from api.price_types.routers import router as price_types_router
from api.prices.routers import router as prices_router
from api.nomenclature.routers import router as nomenclature_router
from api.pictures.routers import router as pictures_router
from api.functions.routers import router as entity_functions_router
from api.units.routers import router as units_router
from api.docs_sales.api.routers import router as docs_sales_router
from api.docs_purchases.routers import router as docs_purchases_router
from api.docs_warehouses.routers import router as docs_warehouses_router
from api.docs_reconciliation.routers import router as docs_reconciliation_router
from api.distribution_docs.routers import router as distribution_docs_router
from api.fifo_settings.routers import router as fifo_settings_router
from api.warehouse_balances.routers import router as warehouse_balances_router
from api.gross_profit_docs.routers import router as gross_profit_docs_router
from api.autosuggestion.routers import router as autosuggestion_router
from api.apple_wallet.routers import router as apple_wallet_router
from api.apple_wallet_card_settings.routers import router as apple_wallet_card_settings_router
from api.loyality_cards.routers import router as loyality_cards
from api.loyality_transactions.routers import router as loyality_transactions
from api.loyality_settings.routers import router as loyality_settings

from apps.amocrm.api.pair.routes import router as amo_pair_router
from apps.amocrm.install.web.routes import router as amo_install_router

from api.integrations.routers import router as int_router
from api.oauth.routes import router as oauth_router
from api.templates.routers import router as templates_router
from api.docs_generate.routers import router as doc_generate_router
from api.webapp.routers import router as webapp_router
from apps.tochka_bank.routes import router as tochka_router
from api.reports.routers import router as reports_router
from apps.evotor.routes import router_auth as evotor_router_auth
from apps.evotor.routes import router as evotor_router
from apps.module_bank.routes import router as module_bank_router
from apps.booking.routers import router as booking_router
from api.settings.amo_triggers.routers import router as triggers_router
from api.trigger_notification.routers import router as triggers_notification
from api.docs_sales_utm_tags.routers import router as utm_router
from api.segments.routers import router as segments_router
from api.tags.routers import router as tags_router
from api.marketplace.routers import router as marketplace_router
from api.tech_cards.router import router as tech_cards_router
from api.tech_operations.router import router as tech_operations_router
from api.settings.cashbox.routers import router as cashbox_settings_router
from api.segments_tags.routers import router as segments_tags_router
from api.employee_shifts.routers import router as employee_shifts_router
from api.feeds.routers import router as feeds_router
from api.chats.routers import router as chats_router
from api.chats.websocket import router as chats_ws_router
from api.chats.rabbitmq_consumer import chat_consumer
from api.chats.avito.avito_routes import router as avito_router
# from api.health.rabbitmq_check import router as rabbitmq_health_router
from api.chats.avito.avito_consumer import avito_consumer
from api.chats.avito.avito_default_webhook import router as avito_default_webhook_router
from scripts.upload_default_apple_wallet_images import DefaultImagesUploader

from jobs.jobs import scheduler

from fastapi import Request
from api.balances.transactions_routers import tinkoff_callback

# sentry_sdk.init(
#     dsn="https://92a9c03cbf3042ecbb382730706ceb1b@sentry.tablecrm.com/4",
#     enable_tracing=True,
#     # Set traces_sample_rate to 1.0 to capture 100%
#     # of transactions for performance monitoring.
#     # We recommend adjusting this value in production,
#     traces_sample_rate=1.0,
# )

app = FastAPI(
    root_path='/api/v1',
    title="TABLECRM API",
    description="Документация API TABLECRM",
    version="1.0",
    # docs_url="/docs",
    # redoc_url="/redoc",
    # openapi_url="/openapi.json"
)

app.add_middleware(GZipMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(tags_router)
app.include_router(triggers_notification)
app.include_router(triggers_router)
app.include_router(booking_router)
app.include_router(evotor_router)
app.include_router(evotor_router_auth)
app.include_router(analytics_router)
app.include_router(cboxes_router)
app.include_router(contragents_router)
app.include_router(payments_router)
app.include_router(transactions_router)
app.include_router(tinkoff_router)
app.include_router(pboxes_router)
app.include_router(projects_router)
app.include_router(articles_router)
app.include_router(users_router)
app.include_router(websockets_router)
app.include_router(installs_router)
app.include_router(balances_router)
app.include_router(cheques_router)
app.include_router(events_router)
app.include_router(amo_pair_router)
app.include_router(amo_install_router)
app.include_router(organizations_router)
app.include_router(contracts_router)
app.include_router(categories_router)
app.include_router(warehouses_router)
app.include_router(price_types_router)
app.include_router(prices_router)
app.include_router(nomenclature_router)
app.include_router(pictures_router)
app.include_router(entity_functions_router)
app.include_router(units_router)
app.include_router(docs_sales_router)
app.include_router(docs_purchases_router)
app.include_router(docs_warehouses_router)
app.include_router(docs_reconciliation_router)
app.include_router(distribution_docs_router)
app.include_router(fifo_settings_router)
app.include_router(warehouse_balances_router)
app.include_router(gross_profit_docs_router)
app.include_router(loyality_cards)
app.include_router(loyality_transactions)
app.include_router(loyality_settings)
app.include_router(cashbox_settings_router)
app.include_router(segments_tags_router)

app.include_router(int_router)
app.include_router(oauth_router)

app.include_router(templates_router)
app.include_router(doc_generate_router)
app.include_router(webapp_router)

app.include_router(tochka_router)
app.include_router(reports_router)

app.include_router(module_bank_router)
app.include_router(utm_router)
app.include_router(segments_router)
app.include_router(marketplace_router)
app.include_router(tech_cards_router)
app.include_router(tech_operations_router)
app.include_router(autosuggestion_router)

app.include_router(employee_shifts_router)
app.include_router(apple_wallet_router)
app.include_router(apple_wallet_card_settings_router)

app.include_router(feeds_router)
app.include_router(chats_router)
app.include_router(chats_ws_router)
# app.include_router(rabbitmq_health_router)
app.include_router(avito_router)
app.include_router(avito_default_webhook_router)


# @app.get("/api/v1/openapi.json", include_in_schema=False)
# async def get_openapi():
#     """Проксировать openapi.json для Swagger UI"""
#     return app.openapi()

@app.get("/health")
async def check_health_app():
    return {"status": "ok"}


@app.post("/api/v1/payments/tinkoff/callback")
@app.get("/api/v1/payments/tinkoff/callback")
async def tinkoff_callback_direct(request: Request):
    return await tinkoff_callback(request)


@app.get("/api/v1/hook/chat/123456", include_in_schema=False)
@app.post("/api/v1/hook/chat/123456", include_in_schema=False)
async def avito_oauth_callback_legacy(
        request: Request,
        code: str = Query(None, description="Authorization code from Avito"),
        state: str = Query(None, description="State parameter for CSRF protection"),
        error: str = Query(None, description="Error from Avito OAuth"),
        error_description: str = Query(None, description="Error description from Avito OAuth"),
        token: str = Query(None, description="Optional user authentication token")
):
    if not code and not state and not error:
        return {"status": "ok", "message": "OAuth callback endpoint is available"}

    if error:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=400,
            detail=f"OAuth error: {error}. {error_description or ''}"
        )

    if not code or not state:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=400,
            detail="Missing required OAuth parameters: code and state are required"
        )

    try:
        from api.chats.avito.avito_routes import avito_oauth_callback
        result = await avito_oauth_callback(code=code, state=state, token=token)
        return result
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise


@app.post("/api/v1/hook/chat/{cashbox_id}", include_in_schema=False)
async def receive_avito_webhook_legacy(cashbox_id: int, request: Request):
    logger = logging.getLogger("main")
    logger.info(f"Webhook received: cashbox_id={cashbox_id}, method={request.method}, path={request.url.path}")

    try:
        from api.chats.avito.avito_handler import AvitoHandler
        from api.chats.avito.avito_types import AvitoWebhook
        from api.chats.avito.avito_webhook import verify_webhook_signature

        body = await request.body()
        signature_header = request.headers.get("X-Avito-Signature")

        if signature_header:
            if not verify_webhook_signature(body, signature_header):
                logger.error("Webhook signature verification failed")
                return {
                    "success": False,
                    "message": "Invalid webhook signature"
                }

        try:
            webhook_data = json.loads(body.decode())
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Failed to parse webhook JSON: {e}")
            return {
                "success": False,
                "message": f"Invalid webhook JSON: {str(e)}"
            }

        logger.debug(f"Webhook data: {json.dumps(webhook_data, default=str)}")

        if not webhook_data:
            logger.error("Empty webhook data")
            return {
                "success": False,
                "message": "Empty webhook data"
            }

        has_id = 'id' in webhook_data
        has_payload = 'payload' in webhook_data
        has_timestamp = 'timestamp' in webhook_data

        if not (has_id or has_payload):
            logger.warning(
                f"Webhook missing required fields. Has id: {has_id}, has payload: {has_payload}, has timestamp: {has_timestamp}")
            logger.warning(f"Webhook keys: {list(webhook_data.keys())}")

        try:
            webhook = AvitoWebhook(**webhook_data)
        except Exception as e:
            logger.error(f"Invalid webhook structure: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Invalid webhook structure: {str(e)}"
            }

        result = await AvitoHandler.handle_webhook_event(webhook, cashbox_id)

        return {
            "success": result.get("success", False),
            "message": result.get("message", "Event processed"),
            "chat_id": result.get("chat_id"),
            "message_id": result.get("message_id")
        }

    except Exception as e:
        logger.error(f"Error processing Avito webhook: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Error: {str(e)}"
        }


@app.middleware("http")
async def write_event_middleware(request: Request, call_next):
    async def set_body(request: Request, body: bytes):
        async def receive() -> Message:
            return {"type": "http.request", "body": body}

        request._receive = receive

    async def get_body(request: Request) -> bytes:
        body = await request.body()
        await set_body(request, body)
        return body

    async def _write_event(request: Request, body: bytes, time_start: float, status_code: int = 500) -> None:
        try:
            if "openapi.json" not in request.url.path:
                token = request.query_params.get("token")
                token = token if token else request.path_params.get("token")

                user_id, cashbox_id = await get_user_id_cashbox_id_by_token(token=token)
                type = "cashevent"
                payload = {} if not body and request.headers.get("content-type") != "application/json" else json.loads(
                    body)
                name = "" if request.scope.get("endpoint") != create_payment else payload.get("type")

                await write_event(
                    type=type,
                    name=name,
                    method=request.method,
                    url=request.url.__str__(),
                    payload=payload,
                    cashbox_id=cashbox_id,
                    user_id=user_id,
                    token=token,
                    ip=request.headers.get("X-Forwarded-For"),
                    status_code=status_code,
                    request_time=time.time() - time_start
                )
        except:
            pass

    time_start = time.time()
    await set_body(request, await request.body())
    body = await get_body(request)
    try:
        response = await call_next(request)
        await _write_event(request=request, body=body, time_start=time_start, status_code=response.status_code)
        return response
    except Exception as e:
        await _write_event(request=request, body=body, time_start=time_start)
        raise e


@app.on_event("startup")
async def startup():
    rabbit_factory = RabbitFactory(settings=RabbitMqSettings(
        rabbitmq_host=os.getenv('RABBITMQ_HOST'),
        rabbitmq_user=os.getenv('RABBITMQ_USER'),
        rabbitmq_pass=os.getenv('RABBITMQ_PASS'),
        rabbitmq_port=os.getenv('RABBITMQ_PORT'),
        rabbitmq_vhost=os.getenv('RABBITMQ_VHOST')
    ))

    s3_factory = S3ServiceFactory(
        s3_settings=S3SettingsModel(
            aws_access_key_id=os.getenv('S3_ACCESS'),
            aws_secret_access_key=os.getenv('S3_SECRET'),
            endpoint_url=os.getenv('S3_URL')
        )
    )

    ioc.set(IRabbitFactory, await rabbit_factory())
    ioc.set(IS3ServiceFactory, s3_factory)

    ioc.set(IBookingEventsRepository, BookingEventsRepository())

    ioc.set(IBookingRepository, BookingRepository())

    ioc.set(IBookingNomenclatureRepository, BookingNomenclatureRepository())

    ioc.set(IWidgetInstallerRepository, WidgetInstallerRepository())
    ioc.set(IYookassaOauthRepository, YookassaOauthRepository())
    ioc.set(IYookassaRequestRepository, YookassaRequestRepository())
    ioc.set(IYookassaPaymentsRepository, YookassaPaymentsRepository())
    ioc.set(IYookassaCrmPaymentsRepository, YookassaCrmPaymentsRepository())

    ioc.set(INomenclatureReader, NomenclatureReader())
    ioc.set(INomenclatureGroupsReader, NomenclatureGroupsReader())
    ioc.set(IAddNomenclatureToGroupFunction, AddNomenclatureToGroupFunction())
    ioc.set(ICreateNomenclatureGroupFunction, CreateNomenclatureGroupFunction())
    ioc.set(IDeleteNomenclatureGroupFunction, DeleteNomenclatureGroupFunction())
    ioc.set(IPatchNomenclatureGroupFunction, PatchNomenclatureGroupFunction())
    ioc.set(IDelNomenclatureFromGroupFunction, DelNomenclatureFromGroupFunction())

    ioc.set(IInsertNomenclatureAttributesFunction, InsertNomenclatureAttributesFunction())
    ioc.set(INomenclatureAttributesReader, NomenclatureAttributesReader())
    ioc.set(IDeleteNomenclatureAttributesFunction, DeleteNomenclatureAttributesFunction())
    ioc.set(IChangeMainNomenclGroupFunction, ChangeMainNomenclGroupFunction())

    InstallCategoriesWeb()(app=app)
    InstallNomenclatureWeb()(app=app)
    ioc.set(IYookasssaAmoTableCrmRepository, YookasssaAmoTableCrmRepository())
    ioc.set(IYookassaTableNomenclature, YookassaTableNomenclature())

    InstallBookingRepeatWeb()(app=app)
    InstallBookingEventsWeb()(app=app)
    InstallWidgetInstallerInfoWeb()(app=app)
    InstallYookassaOauthWeb()(app=app)
    InstallNomenclatureGroupsWeb()(app=app)
    InstallNomenclatureAttributesWeb()(app=app)
    InstallManufacturersWeb()(app=app)
    InstallDocsSalesWeb()(app=app)
    InstallContragentsWeb()(app=app)
    InstallLoyalityTransactionsWeb()(app=app)

    init_db()
    await database.connect()

    if os.getenv("ENABLE_AVITO_ENV_INIT", "false").lower() == "true":
        try:
            from api.chats.avito.avito_init import init_avito_credentials
            await init_avito_credentials()
        except Exception as e:
            pass

    try:
        await chat_consumer.start()
    except Exception as e:
        import traceback
        traceback.print_exc()

    try:
        await avito_consumer.start()
    except Exception as e:
        import traceback
        traceback.print_exc()

    try:
        await DefaultImagesUploader().upload_all()
    except Exception as e:
        pass

    try:
        if not scheduler.running:
            scheduler.start()
    except Exception as e:
        import traceback
        traceback.print_exc()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()
    await chat_consumer.stop()
    await avito_consumer.stop()

    try:
        if scheduler.running:
            scheduler.shutdown()
    except Exception as e:
        pass
