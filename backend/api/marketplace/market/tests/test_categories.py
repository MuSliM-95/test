"""Базовые тесты для API категорий"""
from fastapi.testclient import TestClient

from ..app.main import app

client = TestClient(app)


def test_read_root():
    """Тест корневого endpoint"""
    response = client.get("/")
    assert response.status_code == 200
    assert "message" in response.json()
    assert response.json()["message"] == "Market API"


def test_health_check():
    """Тест проверки здоровья сервиса"""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_get_categories_unauthorized():
    """Тест получения категорий без авторизации (должно работать)"""
    response = client.get("/api/v1/global_categories/")
    # Может быть 200 или 500 (нет БД), но не 403
    assert response.status_code in [200, 500]


def test_create_category_without_admin_key():
    """Тест создания категории без админского ключа"""
    response = client.post(
        "/api/v1/global_categories/",
        json={"name": "Test Category"}
    )
    assert response.status_code == 422  # Missing header


def test_create_category_with_wrong_admin_key():
    """Тест создания категории с неправильным ключом"""
    response = client.post(
        "/api/v1/global_categories/",
        headers={"X-Admin-Key": "wrong-key"},
        json={"name": "Test Category"}
    )
    assert response.status_code == 403
    assert "Недостаточно прав" in response.json()["detail"]


def test_openapi_docs():
    """Тест доступности OpenAPI документации"""
    response = client.get("/docs")
    assert response.status_code == 200

    response = client.get("/redoc")
    assert response.status_code == 200
