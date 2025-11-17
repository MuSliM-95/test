# Market API

Модуль управления публичными категориями для маркетплейса.

## Описание

Market — это приложение для работы с публичными категориями маркетплейса, включая CRUD-операции и загрузку изображений. Добавление и изменение категорий доступно только администраторам через специальный ключ.

## Основные возможности
- Получение списка и дерева категорий
- Получение категории по ID
- Создание, обновление, удаление категории (только для админов)
- Загрузка изображения для категории (только для админов)

## Переменные окружения
Все переменные настраиваются через файл `.env` (см. пример ниже):

```
ADMIN_KEY=your-secret-admin-key-here
DATABASE_URL=postgresql://user:password@localhost:5432/marketplace
DEBUG=False
APP_NAME=Market API
APP_VERSION=1.0.0
API_V1_PREFIX=/api/v1
UPLOAD_DIR=./uploads/categories
MAX_UPLOAD_SIZE=5242880
ALLOWED_EXTENSIONS=.jpg,.jpeg,.png,.gif,.webp
BACKEND_CORS_ORIGINS=*
```

## Запуск

1. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```
2. Проверьте и заполните файл `.env` в папке `tablecrm/backend`.
3. Запустите приложение:
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8000
   ```

## Примеры запросов

- Получить список категорий:
  `GET /api/v1/global_categories/`
- Получить дерево категорий:
  `GET /api/v1/global_categories/tree/`
- Получить категорию по ID:
  `GET /api/v1/global_categories/{id}/`
- Создать категорию (требуется X-Admin-Key):
  `POST /api/v1/global_categories/`
- Загрузить изображение (требуется X-Admin-Key):
  `POST /api/v1/global_categories/{id}/upload_image/`

## Авторизация

Для операций создания, изменения и удаления категорий требуется передавать заголовок:
```
X-Admin-Key: <ваш_админ_ключ>
```

## Контакты
Вопросы и предложения: @yourteam

## Структура проекта

```
market/
├── app/
│   ├── api/
│   │   └── v1/
│   │       ├── dependencies.py         # Проверка admin key
│   │       └── endpoints/
│   │           └── categories.py       # CRUD и загрузка фото для категорий
│   ├── config.py                       # Настройки приложения
│   ├── database/
│   │   └── db.py                       # Модели и подключение к БД
│   ├── main.py                         # Точка входа FastAPI
│   ├── schemas/
│   │   └── category.py                 # Pydantic-схемы категорий
│   └── utils/
│       └── helpers.py                  # Вспомогательные функции
├── tests/
│   └── test_categories.py              # Тесты API категорий
├── uploads/                            # Категорийные фото (игнорируется в git)
├── .env.example                        # Пример переменных окружения
├── .gitignore                          # Исключения для git
├── README.md                           # Документация
├── requirements.txt                    # Зависимости
```
