"""Главная точка входа FastAPI приложения"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .api.v1.endpoints import categories
from .config import settings
from .database.db import database


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Startup: подключение к базе данных
    await database.connect()
    yield
    # Shutdown: отключение от базы данных
    await database.disconnect()


# Создаём FastAPI приложение
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="API для управления категориями маркетплейса",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Настраиваем CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключаем статические файлы для загрузок
if settings.UPLOAD_DIR.exists():
    app.mount(
        "/uploads",
        StaticFiles(directory=settings.UPLOAD_DIR.parent),
        name="uploads"
    )


# Подключаем роутеры
app.include_router(
    categories.router,
    prefix=f"{settings.API_V1_PREFIX}/mp",
    tags=["categories"]
)


@app.get("/")
async def root():
    """Корневой endpoint"""
    return {
        "message": "Market API",
        "version": settings.APP_VERSION,
        "docs": "/docs",
        "redoc": "/redoc"
    }


@app.get("/health")
async def health_check():
    """Проверка состояния сервиса"""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG
    )
