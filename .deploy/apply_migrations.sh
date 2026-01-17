#!/bin/env bash
set -eu

POSTGRES_USER=${POSTGRES_USER:-cash_2_user}
POSTGRES_PASS=${POSTGRES_PASS:-secret}
POSTGRES_HOST=${POSTGRES_HOST:-db}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
IMAGE_NAME=${IMAGE_NAME:-tablecrm_backend:latest}
NETWORK_NAME=${NETWORK_NAME:-infrastructure}

echo "Применяем миграции Alembic..."

TEMP_CONTAINER="alembic_temp_$(date +%s)"
docker run -d \
  --name "$TEMP_CONTAINER" \
  --network "$NETWORK_NAME" \
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

# Получаем все head
HEADS=$(run_alembic heads --verbose 2>/dev/null | grep -o '^[a-f0-9]\+' | tr '\n' ' ')

if [ -n "$HEADS" ] && [ $(echo "$HEADS" | wc -w) -gt 1 ]; then
  echo "Обнаружено несколько head: $HEADS. Выполняем объединение..."
  # Объединяем ВСЕ head в одну миграцию
  run_alembic merge -m "Auto-merge $(echo "$HEADS" | wc -w) heads" $HEADS
  echo "Миграции объединены"
fi

# Применяем все миграции
echo "Применяем миграции..."
run_alembic upgrade head
echo "Все миграции успешно применены"

docker rm -f "$TEMP_CONTAINER" >/dev/null
