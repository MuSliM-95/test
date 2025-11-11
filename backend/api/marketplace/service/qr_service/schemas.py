from datetime import datetime

from pydantic import BaseModel

from api.marketplace.service.qr_service.constants import QrEntityTypes


class QRResolveResponse(BaseModel):
    """Ответ QR-резолвера"""
    type: QrEntityTypes  # "product" или "location"
    entity: dict  # Данные товара или локации
    qr_hash: str
    resolved_at: datetime
