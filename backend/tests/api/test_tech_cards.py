import pytest
from httpx import ASGITransport, AsyncClient
from backend.main import app
import pytest_asyncio


class TestTechCardsAPI:
    @pytest_asyncio.fixture(scope="class", autouse=True)
    async def client(self):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://localhost/tech_cards"
        ) as ac:
            yield ac

    @pytest.mark.asyncio
    async def test_get_empty_list(self, client: AsyncClient):
        response = await client.get("/")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    @pytest.mark.asyncio
    async def test_create_and_get_tech_card(self, client: AsyncClient):
        payload = {
            "name": "Test Card",
            "card_type": "reference",
            "user_id": 1,
            "items": [{"name": "Item1", "quantity": 2, "unit": "kg"}],
        }
        response = await client.post("/", json=payload)
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == payload["name"]
        tech_card_id = data["id"]

        # get by id
        response = await client.get(f"/{tech_card_id}")
        assert response.status_code == 200
        assert response.json()["id"] == tech_card_id

    @pytest.mark.asyncio
    async def test_update_tech_card(self, client: AsyncClient):
        # create
        payload = {
            "name": "ToUpdate",
            "card_type": "reference",
            "user_id": 1,
            "items": [],
        }
        response = await client.post("/", json=payload)
        tech_card_id = response.json()["id"]

        # update
        update_payload = {
            "name": "UpdatedName",
            "card_type": "reference",
            "user_id": 1,
            "items": [],
        }
        response = await client.put(f"/{tech_card_id}", json=update_payload)
        assert response.status_code == 200
        assert response.json()["name"] == "UpdatedName"

    @pytest.mark.asyncio
    async def test_delete_tech_card(self, client: AsyncClient):
        # create
        payload = {
            "name": "ToDelete",
            "card_type": "reference",
            "user_id": 1,
            "items": [],
        }
        response = await client.post("/", json=payload)
        tech_card_id = response.json()["id"]

        # delete
        response = await client.delete(f"/{tech_card_id}")
        assert response.status_code == 204

        # check not found
        response = await client.get(f"/{tech_card_id}")
        assert response.status_code == 404
