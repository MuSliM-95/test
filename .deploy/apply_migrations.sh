#!/bin/env bash
set -eu

POSTGRES_USER=${POSTGRES_USER:-cash_2_user}
POSTGRES_PASS=${POSTGRES_PASS:-secret}
POSTGRES_HOST=${POSTGRES_HOST:-db}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
IMAGE_NAME=${IMAGE_NAME:-tablecrm_backend:latest}

echo "Применяем миграции Alembic..."

TEMP_CONTAINER="alembic_temp_$(date +%s)"
docker run -d \
  --name "$TEMP_CONTAINER" \
  --network infrastructure \
  -e POSTGRES_USER="$POSTGRES_USER" \
  -e POSTGRES_PASS="$POSTGRES_PASS" \
  -e POSTGRES_HOST="$POSTGRES_HOST" \
  -e POSTGRES_PORT="$POSTGRES_PORT" \
  --entrypoint sleep \
  "$IMAGE_NAME" \
  300

until docker exec "$TEMP_CONTAINER" ls /backend/alembic.ini &>/dev/null; do
  echo "Ожидание запуска временного контейнера..."
  sleep 2
done

run_alembic() {
  docker exec "$TEMP_CONTAINER" python -m alembic "$@"
}

HEADS=$(run_alembic heads --verbose 2>/dev/null | wc -l)
if [ "$HEADS" -gt 1 ]; then
  echo "Обнаружено несколько head ($HEADS). Выполняем объединение..."
  run_alembic merge -m "Auto-merge heads during CI/CD deployment"
  echo "Миграции объединены"
fi

CURRENT=$(run_alembic current --verbose 2>/dev/null)
HEAD=$(run_alembic heads 2>/dev/null | head -n1 | cut -d' ' -f1)

if [ -n "$HEAD" ] && [ "$CURRENT" != "$HEAD" ]; then
  echo "Обнаружены неприменённые миграции. Применяем..."
  run_alembic upgrade head
  echo "Все миграции успешно применены"
else
  echo "Миграции в актуальном состоянии"
fi

docker rm -f "$TEMP_CONTAINER" >/dev/null