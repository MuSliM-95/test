import pytest
from httpx import ASGITransport, AsyncClient
from backend.main import app
import pytest_asyncio
import uuid


token = "c16ff521c6c5dcb215a84aa2e7bc8c5d08073abba25ae45e5e476b71cc5e9205"


class TestTechOperationsAPI:
    @pytest_asyncio.fixture(scope="class", autouse=True)
    async def client(self):
        await app.router.startup()
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://localhost/tech_operations",
        ) as ac:
            yield ac
        await app.router.shutdown()

    @pytest.mark.asyncio
    async def test_get_empty_operations(self, client: AsyncClient):
        response = await client.get(
            "/",
            params={"token": token},
        )
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    @pytest.mark.asyncio
    async def test_create_and_get_tech_operation(self, client: AsyncClient):
        # TODO: Create numeclature
        payload = {
            "name": "Test Card",
            "card_type": "reference",
            "items": [{"name": "Item1", "quantity": 2}],
        }
        response = await client.post(
            url="http://localhost/tech_cards/",
            json=payload,
            params={"token": token},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == payload["name"]
        tech_card_id = data["id"]

        payload = {
            "tech_card_id": tech_card_id,
            "output_quantity": 10,
            "from_warehouse_id": str(uuid.uuid4()),
            "to_warehouse_id": str(uuid.uuid4()),
            "nomenclature_id": 1,
            "component_quantities": [{"name": "Component1", "quantity": 5}],
            "payment_ids": [],
        }
        response = await client.post(
            "/",
            json=payload,
            params={"token": token},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["tech_card_id"] == str(tech_card_id)

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_operation(self, client: AsyncClient):
        fake_id = str(uuid.uuid4())
        response = await client.post(
            f"/{fake_id}/cancel",
            params={"token": token},
        )
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_nonexistent_operation(self, client: AsyncClient):
        fake_id = str(uuid.uuid4())
        response = await client.delete(
            f"/{fake_id}",
            params={"token": token},
        )
        assert response.status_code == 204 or response.status_code == 404
