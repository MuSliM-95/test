

from datetime import datetime


def serialize_datetime_fields(record):
    """
    Преобразует поля created_at и updated_at в isoformat,
    если они есть в record.
    Поддерживает record как dict или sqlalchemy Row.
    """
    result = dict(record)
    for field in ("created_at", "updated_at"):
        value = result.get(field)
        if value is not None and isinstance(value, datetime):
            result[field] = value.isoformat()
    return result
