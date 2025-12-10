import datetime
import hashlib
import random
import string
import sys

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

sys.path.insert(0, "/backend")
from backend.main import app
from database.db import (
    cboxes,
    contragents,
    database,
    loyality_cards,
    users,
    users_cboxes_relation,
)


def generate_random_string(length=8):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


def generate_random_phone():
    return f"79{''.join(random.choices(string.digits, k=9))}"


class TestPromocodesAPI:
    @pytest_asyncio.fixture(scope="function")
    async def client(self):
        await app.router.startup()
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://localhost",
            follow_redirects=True,
        ) as ac:
            yield ac
        await app.router.shutdown()

    @pytest_asyncio.fixture(scope="function")
    async def admin_user(self):
        chat_id = str(random.randint(100000000, 999999999))
        phone = generate_random_phone()
        first_name = f"TestUser_{generate_random_string(4)}"

        query_user = (
            users.insert()
            .values(
                chat_id=chat_id,
                first_name=first_name,
                username=f"user_{chat_id}",
                phone_number=phone,
                created_at=int(datetime.datetime.now().timestamp()),
                updated_at=int(datetime.datetime.now().timestamp()),
            )
            .returning(users.c.id)
        )
        user_id = await database.execute(query_user)

        query_cbox = (
            cboxes.insert()
            .values(
                name=f"Касса {first_name}",
                balance=0.0,
                created_at=int(datetime.datetime.now().timestamp()),
                updated_at=int(datetime.datetime.now().timestamp()),
            )
            .returning(cboxes.c.id)
        )
        cashbox_id = await database.execute(query_cbox)

        raw_token = f"{chat_id}{datetime.datetime.now()}"
        token = hashlib.sha256(raw_token.encode()).hexdigest()

        query_rel = users_cboxes_relation.insert().values(
            user=user_id,
            cashbox_id=cashbox_id,
            token=token,
            is_owner=True,
            status=True,
            created_at=int(datetime.datetime.now().timestamp()),
            updated_at=int(datetime.datetime.now().timestamp()),
        )
        relation_id = await database.execute(query_rel)

        return {
            "token": token,
            "user_id": user_id,
            "cashbox_id": cashbox_id,
            "relation_id": relation_id,
            "phone": phone,
        }

    @pytest_asyncio.fixture(scope="function")
    async def contragent_id(self, client, admin_user):
        token = admin_user["token"]
        phone = generate_random_phone()

        payload = {
            "name": f"TestClient_{phone}",
            "phone": f"+{phone}",
            "description": "Test contragent",
            "external_id": f"ext_{generate_random_string(6)}",
        }

        response = await client.post(
            "/contragents/", json=payload, params={"token": token}
        )

        assert response.status_code in [200, 201]
        data = response.json()
        return data[0]["id"] if isinstance(data, list) else data["id"]

    @pytest_asyncio.fixture(scope="function")
    async def organization_id(self, client, admin_user):
        token = admin_user["token"]

        payload = {
            "type": "OOO",
            "short_name": f"TestOrg{generate_random_string(4)}",
            "full_name": "Test Organization LLC",
            "inn": 1234567890,
        }

        response = await client.post(
            "/organizations/", json=payload, params={"token": token}
        )

        assert response.status_code in [200, 201]
        return response.json()["id"]

    @pytest_asyncio.fixture(scope="function")
    async def card_data(self, client, admin_user, contragent_id, organization_id):
        query_contragent = contragents.select().where(contragents.c.id == contragent_id)
        contragent = await database.fetch_one(query_contragent)
        phone = contragent.phone
        card_number = int(phone.replace("+", ""))

        query_card = (
            loyality_cards.insert()
            .values(
                card_number=card_number,
                contragent_id=contragent_id,
                organization_id=organization_id,
                cashbox_id=admin_user["cashbox_id"],
                created_by_id=admin_user["relation_id"],
                balance=0.0,
                status_card=True,
                is_deleted=False,
                apple_wallet_advertisement="TEST",
            )
            .returning(loyality_cards.c.id)
        )
        card_id = await database.execute(query_card)

        return {"card_id": card_id, "phone": phone, "card_number": str(card_number)}

    @pytest.mark.asyncio
    async def test_smk01_create_promocode(self, client, admin_user, organization_id):
        token = admin_user["token"]
        code = generate_random_string(8).upper()

        payload = {
            "code": code,
            "points_amount": 100.0,
            "organization_id": organization_id,
            "type": "permanent",
            "max_usages": 10,
            "is_active": True,
        }

        response = await client.post(
            "/promocodes/", json=payload, params={"token": token}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["code"] == code
        assert data["points_amount"] == 100.0

    @pytest.mark.asyncio
    async def test_smk02_get_list(self, client, admin_user, organization_id):
        token = admin_user["token"]
        code = generate_random_string(8).upper()

        await client.post(
            "/promocodes/",
            json={
                "code": code,
                "points_amount": 50,
                "organization_id": organization_id,
            },
            params={"token": token},
        )

        response = await client.get("/promocodes/", params={"token": token})

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert any(p["code"] == code for p in data)

    @pytest.mark.asyncio
    async def test_smk03_run_promocode(
        self, client, admin_user, organization_id, card_data
    ):
        token = admin_user["token"]
        promocode = generate_random_string(8).upper()

        await client.post(
            "/promocodes/",
            json={
                "code": promocode,
                "points_amount": 150.0,
                "organization_id": organization_id,
                "is_active": True,
            },
            params={"token": token},
        )

        phone = card_data["phone"].replace("+", "")
        response = await client.post(
            "/promocodes/run/",
            json={"code": promocode, "phone_number": phone},
            params={"token": token},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["added_points"] == 150.0

    @pytest.mark.asyncio
    async def test_smk04_delete_promocode(self, client, admin_user, organization_id):
        token = admin_user["token"]
        code = generate_random_string(8).upper()

        resp_create = await client.post(
            "/promocodes/",
            json={
                "code": code,
                "points_amount": 50,
                "organization_id": organization_id,
            },
            params={"token": token},
        )
        promo_id = resp_create.json()["id"]

        resp_delete = await client.delete(
            f"/promocodes/{promo_id}/", params={"token": token}
        )

        assert resp_delete.status_code == 200
        assert resp_delete.json()["is_active"] is False

    @pytest.mark.asyncio
    async def test_smk05_get_by_id(self, client, admin_user, organization_id):
        token = admin_user["token"]
        code = generate_random_string(8).upper()

        resp_create = await client.post(
            "/promocodes/",
            json={
                "code": code,
                "points_amount": 75.0,
                "organization_id": organization_id,
            },
            params={"token": token},
        )
        promo_id = resp_create.json()["id"]

        response = await client.get(f"/promocodes/{promo_id}/", params={"token": token})

        assert response.status_code == 200
        assert response.json()["code"] == code

    @pytest.mark.asyncio
    async def test_smk06_update_promocode(self, client, admin_user, organization_id):
        token = admin_user["token"]
        code = generate_random_string(8).upper()

        resp_create = await client.post(
            "/promocodes/",
            json={
                "code": code,
                "points_amount": 100.0,
                "organization_id": organization_id,
            },
            params={"token": token},
        )
        promo_id = resp_create.json()["id"]

        response = await client.patch(
            f"/promocodes/{promo_id}/",
            json={"points_amount": 200.0, "is_active": False},
            params={"token": token},
        )

        assert response.status_code == 200
        assert response.json()["points_amount"] == 200.0
        assert response.json()["is_active"] is False

    @pytest.mark.asyncio
    async def test_smk07_run_nonexistent(self, client, admin_user, card_data):
        token = admin_user["token"]
        phone = card_data["phone"].replace("+", "")

        response = await client.post(
            "/promocodes/run/",
            json={"code": "FAKE_CODE_999", "phone_number": phone},
            params={"token": token},
        )

        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_smk08_duplicate_error(self, client, admin_user, organization_id):
        token = admin_user["token"]
        code = generate_random_string(8).upper()

        payload = {
            "code": code,
            "points_amount": 100,
            "organization_id": organization_id,
        }

        await client.post("/promocodes/", json=payload, params={"token": token})

        resp = await client.post("/promocodes/", json=payload, params={"token": token})
        assert resp.status_code == 400
