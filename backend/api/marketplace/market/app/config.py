"""Конфигурация приложения"""
from pathlib import Path
from typing import List

from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Настройки приложения загружаемые из переменных окружения"""

    # Универсальный путь к .env (ищет в tablecrm/backend)
    import os
    BASE_DIR = Path(__file__).resolve().parents[4]  # tablecrm/backend
    model_config = ConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

    # Настройки API
    APP_NAME: str = "Market API"
    APP_VERSION: str = "1.0.0"
    API_V1_PREFIX: str = "/api/v1"
    DEBUG: bool = False

    # Безопасность
    ADMIN_KEY: str = "your-secret-admin-key-here"

    # База данных
    DATABASE_URL: str = "postgresql://user:password@localhost:5432/marketplace"

    # Загрузка файлов
    UPLOAD_DIR: Path = Path("./uploads/categories")
    MAX_UPLOAD_SIZE: int = 5 * 1024 * 1024  # 5MB
    ALLOWED_EXTENSIONS: set = {".jpg", ".jpeg", ".png", ".gif", ".webp"}

    # CORS
    BACKEND_CORS_ORIGINS: List[str] = ["*"]


# Создаём глобальный экземпляр настроек
settings = Settings()

# Создаём папку для загрузок если её нет
settings.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
