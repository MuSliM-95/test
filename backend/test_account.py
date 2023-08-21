from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_get_account():
    response_1 = client.get("/ping/")
    assert response_1.status_code == 200
    assert response_1.json() == {"ping": "pong"}
