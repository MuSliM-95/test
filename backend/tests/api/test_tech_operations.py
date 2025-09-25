import pytest
from httpx import ASGITransport, AsyncClient
from backend.main import app
import pytest_asyncio
import uuid


token = "c9e7c8072c900d07aadccabe66fcbae873d01807d176d3353454edc9091fd244"


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
    async def test_get_tech_operations(self, client: AsyncClient):
        response = await client.get(
            "/",
            params={"token": token},
        )
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    @pytest.mark.asyncio
    async def test_create_and_get_tech_operation(self, client: AsyncClient):
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

        # Create tech card
        payload_tech_card = {
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
        response_tech_card = await client.post(
            url="http://localhost/tech_cards/",
            json=payload_tech_card,
            params={"token": token},
        )
        assert response_tech_card.status_code == 201
        data_tech_card = response_tech_card.json()
        assert data_tech_card["name"] == payload_tech_card["name"]

        # Create tech operation
        tech_card_id = data_tech_card["id"]
        payload_tech_operation = {
            "tech_card_id": tech_card_id,
            "output_quantity": 10,
            "from_warehouse_id": str(uuid.uuid4()),
            "to_warehouse_id": str(uuid.uuid4()),
            # "nomenclature_id": data_nomenclature[0]["id"],
            "component_quantities": [{"name": "Component1", "quantity": 5}],
            "payment_ids": [],
        }
        response_tech_operation = await client.post(
            "/",
            json=payload_tech_operation,
            params={"token": token},
        )
        assert response_tech_operation.status_code == 201
        data = response_tech_operation.json()
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
