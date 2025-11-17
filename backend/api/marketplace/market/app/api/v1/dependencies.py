"""API зависимости для аутентификации и авторизации"""
from fastapi import Header, HTTPException

from ...config import settings


async def verify_admin_key(x_admin_key: str = Header(...)):
    """
    Проверка админского ключа API из заголовка X-Admin-Key

    Args:
        x_admin_key: Админский ключ из заголовка запроса

    Raises:
        HTTPException: 403 если ключ неверный

    Returns:
        bool: True если ключ верный
    """
    if x_admin_key != settings.ADMIN_KEY:
        raise HTTPException(
            status_code=403,
            detail="Недостаточно прав. Требуется админский ключ."
        )
    return True
