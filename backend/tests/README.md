## Подготовка

У вас должены работать backend и инфраструктура.

## Запуск тестов

Запуск тестов на примере `tests/api/test_promocods.py`:

```
docker-compose -f docker-compose-dev.yml exec backend pytest tests/api/test_promocods.py
```
