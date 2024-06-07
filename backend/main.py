import json
import time

from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from database.db import database
from database.fixtures import init_db
from jobs import scheduler
import sentry_sdk

from functions.users import get_user_id_cashbox_id_by_token
from functions.events import write_event

from starlette.types import Message

from apps.evotor.routes import has_access

from api.cashboxes.routers import router as cboxes_router
from api.contragents.routers import router as contragents_router
from api.payments.routers import create_payment, router as payments_router
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
from api.manufacturers.routers import router as manufacturers_router
from api.price_types.routers import router as price_types_router
from api.prices.routers import router as prices_router
from api.nomenclature.routers import router as nomenclature_router
from api.pictures.routers import router as pictures_router
from api.functions.routers import router as entity_functions_router
from api.units.routers import router as units_router
from api.docs_sales.routers import router as docs_sales_router
from api.docs_purchases.routers import router as docs_purchases_router
from api.docs_warehouses.routers import router as docs_warehouses_router
from api.docs_reconciliation.routers import router as docs_reconciliation_router
from api.distribution_docs.routers import router as distribution_docs_router
from api.fifo_settings.routers import router as fifo_settings_router
from api.warehouse_balances.routers import router as warehouse_balances_router
from api.gross_profit_docs.routers import router as gross_profit_docs_router

from api.loyality_cards.routers import router as loyality_cards
from api.loyality_transactions.routers import router as loyality_transactions
from api.loyality_settings.routers import router as loyality_settings

from apps.amocrm.api.pair.routes import router as amo_pair_router
from apps.amocrm.api.install.routes import router as amo_install_router

from api.integrations.routers import router as int_router
from api.oauth.routes import router as oauth_router
from api.templates.routers import router as templates_router
from api.docs_generate.routers import router as doc_generate_router
from api.webapp.routers import router as webapp_router
from apps.tochka_bank.routes import router as tochka_router
from api.reports.routers import router as reports_router
from apps.evotor.routes import router_auth as evotor_router_auth
from apps.evotor.routes import router as evotor_router


sentry_sdk.init(
    dsn="https://92a9c03cbf3042ecbb382730706ceb1b@sentry.tablecrm.com/4",
    enable_tracing=True,
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production,
    traces_sample_rate=1.0,
)

app = FastAPI(
    root_path="/api/v1",
    title="TABLECRM API",
    description="Документация API TABLECRM",
    version="1.0"
)

app.add_middleware(GZipMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(evotor_router)
app.include_router(evotor_router_auth)
app.include_router(analytics_router)
app.include_router(cboxes_router)
app.include_router(contragents_router)
app.include_router(payments_router)
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
app.include_router(manufacturers_router)
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

app.include_router(int_router)
app.include_router(oauth_router)

app.include_router(templates_router)
app.include_router(doc_generate_router)
app.include_router(webapp_router)

app.include_router(tochka_router)
app.include_router(reports_router)



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
                payload = {} if not body and request.headers.get("content-type") != "application/json" else json.loads(body)
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
        except: pass

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
    init_db()
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()
    if scheduler.get_job("check_account"):
        scheduler.remove_job("check_account")
    if scheduler.get_job("autoburn"):
        scheduler.remove_job("autoburn")
    if scheduler.get_job("repeat_payments"):
        scheduler.remove_job("repeat_payments")
    if scheduler.get_job("distribution"):
        scheduler.remove_job("distribution")


scheduler.start()
