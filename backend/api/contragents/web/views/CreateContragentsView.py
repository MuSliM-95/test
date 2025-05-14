from datetime import datetime
from typing import Union, List

import phonenumbers
from fastapi import HTTPException
from fastapi.responses import Response
from phonenumbers import geocoder
from sqlalchemy import select

import api.contragents.schemas as ca_schemas
from database.db import database, users_cboxes_relation, contragents
from ws_manager import manager


class CreateContragentsView:

    async def __call__(self, token: str, body: Union[ca_schemas.ContragentCreate, List[ca_schemas.ContragentCreate]]):
        user = await database.fetch_one(
            users_cboxes_relation.select(users_cboxes_relation.c.token == token)
        )
        if not user:
            raise HTTPException(403, "Неверный токен")
        if not user.status:
            raise HTTPException(403, "Неверный токен")

        items = body if isinstance(body, list) else [body]
        insert_values, phones_seen = [], set()

        for item in items:
            data = item.dict(exclude_unset=True)

            phone_number = data['phone']
            phone_code = None
            is_phone_formatted = False

            if phone_number:
                try:
                    phone_number_with_plus = f"+{phone_number}" if not phone_number.startswith("+") else phone_number
                    number_phone_parsed = phonenumbers.parse(phone_number_with_plus, "RU")
                    phone_number = phonenumbers.format_number(number_phone_parsed,
                                                              phonenumbers.PhoneNumberFormat.E164)
                    phone_code = geocoder.description_for_number(number_phone_parsed, "en")
                    is_phone_formatted = True
                    if not phone_code:
                        phone_number = data['phone']
                        is_phone_formatted = False
                except:
                    try:
                        number_phone_parsed = phonenumbers.parse(phone_number, "RU")
                        phone_number = phonenumbers.format_number(number_phone_parsed,
                                                                  phonenumbers.PhoneNumberFormat.E164)
                        phone_code = geocoder.description_for_number(number_phone_parsed, "en")
                        is_phone_formatted = True
                        if not phone_code:
                            phone_number = data['phone']
                            is_phone_formatted = False
                    except:
                        phone_number = data['phone']
                        is_phone_formatted = False

            if phone_number in phones_seen:
                raise HTTPException(status_code=400, detail={
                    "message": "Phone number already in use",
                    "phones": [phone_number]
                })

            phones_seen.add(phone_number)

            insert_values.append(
                {
                    "name": data.get("name", ""),
                    "external_id": data.get("external_id", ""),
                    "inn": data.get("inn", ""),
                    "phone": phone_number,
                    "phone_code": phone_code,
                    "is_phone_formatted": is_phone_formatted,
                    "description": data.get("description"),
                    "contragent_type": data.get("contragent_type"),
                    "birth_date": data.get("birth_date"),
                    "data": data.get("data"),
                    "cashbox": user.cashbox_id,
                    "is_deleted": False,
                    "created_at": int(datetime.now().timestamp()),
                    "updated_at": int(datetime.now().timestamp()),
                    "email": data.get("email"),
                }
            )

        if not insert_values:
            return Response(status_code=204)

        phones_to_check = {p["phone"] for p in insert_values if p["phone"]}
        existing_phones: set[str] = set()
        if phones_to_check:
            rows = await database.fetch_all(
                select(contragents.c.phone).where(
                    contragents.c.cashbox == user.cashbox_id,
                    contragents.c.phone.in_(phones_to_check),
                    contragents.c.is_deleted.is_(False),
                )
            )
            existing_phones = {r.phone for r in rows}

        duplicated_contragent_phones = [p for p in insert_values if p["phone"] in existing_phones]

        if duplicated_contragent_phones:
            raise HTTPException(status_code=400, detail={
                "message": "Phone number already in use",
                "phones": [p["phone"] for p in duplicated_contragent_phones]
            })
        query = (
            contragents.insert()
            .values(insert_values)
            .returning(contragents.c.id)
        )
        ids = await database.fetch_all(query=query)

        rows = await database.fetch_all(select(contragents).where(contragents.c.id.in_([k.id for k in ids])))
        for r in rows:
            await manager.send_message(
                token, {"action": "create", "target": "contragents", "result": dict(r)}
            )

        return rows if isinstance(body, list) else rows[0]