import pytest
from httpx import ASGITransport, AsyncClient
from backend.main import app
import pytest_asyncio

# TODO: Token should be generated dynamically or mocked
token = "c9e7c8072c900d07aadccabe66fcbae873d01807d176d3353454edc9091fd244"


class TestTechCardsAPI:
    @pytest_asyncio.fixture(scope="class", autouse=True)
    async def client(self):
        await app.router.startup()
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://localhost/tech_cards"
        ) as ac:
            yield ac
        await app.router.shutdown()

    @pytest.mark.asyncio
    async def test_get_tech_cards(self, client: AsyncClient):
        response = await client.get(
            "/",
            params={"token": token},
        )
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    @pytest.mark.asyncio
    async def test_create_and_get_tech_card(self, client: AsyncClient):
        # TODO: исправить ошибку при создании номенклатуры - cannot perform operation: another operation is in progress
        # Create numeclature
        # payload_nomenclature = [
        #     {
        #         "name": "Test Nomenclature",
        #     }
        # ]
        # response_nomenclature = await client.post(
        #     url="http://localhost/nomenclature/",
        #     json=payload_nomenclature,
        #     params={"token": token},
        # )
        # data_nomenclature = response_nomenclature.json()
        # assert response_nomenclature.status_code == 201

        payload = {
            "name": "Test Card",
            "card_type": "reference",
            "items": [
                # {
                #     "name": "Item1",
                #     "quantity": 2,
                #     "nomenclature_id": data_nomenclature[0]["id"],
                # }
            ],
        }
        response = await client.post(
            "/",
            json=payload,
            params={"token": token},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == payload["name"]
        tech_card_id = data["id"]

        # get by id
        response = await client.get(
            f"/{tech_card_id}",
            params={"token": token},
        )
        assert response.status_code == 200
        assert response.json()["id"] == tech_card_id

    @pytest.mark.asyncio
    async def test_update_tech_card(self, client: AsyncClient):
        # create
        payload = {
            "name": "ToUpdate",
            "card_type": "reference",
            "items": [],
        }
        response = await client.post(
            "/",
            json=payload,
            params={"token": token},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == payload["name"]
        tech_card_id = data["id"]

        # update
        update_payload = {
            "name": "UpdatedName",
            "card_type": "reference",
            "items": [],
        }
        response = await client.put(
            f"/{tech_card_id}",
            json=update_payload,
            params={"token": token},
        )
        assert response.status_code == 200
        assert response.json()["name"] == "UpdatedName"

    @pytest.mark.asyncio
    async def test_delete_tech_card(self, client: AsyncClient):
        # create
        payload = {
            "name": "ToDelete",
            "card_type": "reference",
            "items": [],
        }
        response = await client.post(
            "/",
            json=payload,
            params={"token": token},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == payload["name"]
        tech_card_id = data["id"]

        # delete
        response = await client.delete(
            f"/{tech_card_id}",
            params={"token": token},
        )
        assert response.status_code == 204

        # check not found
        response = await client.get(
            f"/{tech_card_id}",
            params={"token": token},
        )
        assert response.status_code == 404
