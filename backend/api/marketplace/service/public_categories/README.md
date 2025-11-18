# Быстрый старт: public_categories

## Быстрый запуск скрипта загрузки категорий

1. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```
2. Скопируйте `.env.example` в `.env` и заполните переменные для подключения к вашей базе данных PostgreSQL.
3. Запустите скрипт:
   ```bash
   python load_avito_categories.py - Windows
   python3 load_avito_categories.py - Linux/Mac

   ```

## Быстрый запуск сервиса public_categories_service.py

1. Убедитесь, что зависимости установлены и переменные окружения заданы (см. выше).
2. Запустите сервис:
   ```bash
   python public_categories_service.py
   ```

---

- Скрипт загрузит категории Авито в таблицу `global_categories` вашей базы.
- Для работы требуется доступ к PostgreSQL и корректно настроенный SQLAlchemy.
- Для запуска в Docker используйте:
   ```bash
   docker compose -f <ваш_compose_файл> exec backend python /backend/api/marketplace/service/public_categories/load_avito_categories.py
   ```
