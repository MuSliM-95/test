"""Вспомогательные функции"""
from datetime import datetime


def datetime_to_timestamp(record):
    """Конвертация datetime полей в timestamp строки"""
    record_dict = dict(record)

    if "created_at" in record_dict and isinstance(
        record_dict["created_at"], datetime
    ):
        record_dict["created_at"] = str(
            int(record_dict["created_at"].timestamp())
        )

    if "updated_at" in record_dict and isinstance(
        record_dict["updated_at"], datetime
    ):
        record_dict["updated_at"] = str(
            int(record_dict["updated_at"].timestamp())
        )

    return record_dict
