from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from database.db import database
from database.fixtures import init_db
from jobs import scheduler
import json
from functions.users import get_user_id_cashbox_id_by_token
from functions.events import write_event

from starlette.types import Message

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

from apps.amocrm.routes import router as amo_router

from api.integrations.routers import router as int_router
from api.oauth.routes import router as oauth_router
from api.ping.routers import router as pingpong_router



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
app.include_router(amo_router)
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
app.include_router(pingpong_router)


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

    await set_body(request, await request.body())
    body = await get_body(request)

    try:
        if "docs" not in request.url.path and "openapi.json" not in request.url.path:
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
                ip=request.headers.get("x-forwarded-for")
            )
    except:
        pass

    response = await call_next(request)

    return response


@app.on_event("startup")
async def startup():
    init_db()
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()
    scheduler.remove_all_jobs()


scheduler.start()
