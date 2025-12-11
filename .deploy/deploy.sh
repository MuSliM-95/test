#!/bin/env bash

set -xue

SERVICE_NAME="backend"
IMAGE_NAME="git.tablecrm.com:5050/tablecrm/tablecrm/backend:$CI_COMMIT_SHA"
NGINX_CONTAINER_NAME="nginx"
UPSTREAM_CONF_PATH="/etc/nginx/dir/upstream.conf"
BOT_SERVICE_NAME="telegram_bot"
BOT_CONTAINER_NAME_PREFIX="telegram_bot"

reload_nginx() {
  docker exec $NGINX_CONTAINER_NAME nginx -s reload
}

update_upstream_conf() {
  NEW_PORT=$1

  new_container_id=$(docker ps -f name="${SERVICE_NAME}_$NEW_PORT" -q | head -n1)
  new_container_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{"\n"}}{{end}}' "$new_container_id" | head -n1)

  echo "Обновление upstream.conf для указания порта $NEW_PORT и ip ${new_container_ip}"

  echo "server ${new_container_ip}:8000;" > upstream.conf.tmp

  docker cp upstream.conf.tmp $NGINX_CONTAINER_NAME:$UPSTREAM_CONF_PATH

  rm upstream.conf.tmp
}

deploy_new_version() {
  local current_ports new_port container_name max_retries=30 retry_interval=1

  echo "Начало деплоя новой версии сервиса..."

  # Получаем порты работающих контейнеров
  current_ports=$(docker ps --filter "name=$SERVICE_NAME" \
      --filter "status=running" \
      --format '{{.Ports}}' | \
      grep -oP '0\.0\.0\.0:\K[0-9]+' | \
      sort -n)

  # Определяем порт для нового контейнера
  if echo "$current_ports" | grep -q "8000"; then
      new_port=8002
  else
      new_port=8000
  fi

  container_name="${SERVICE_NAME}_${new_port}"

  echo "Деплой новой версии на порт $new_port (контейнер: $container_name)"

  # Проверка обязательных переменных
  local required_vars=("IMAGE_NAME" "SERVICE_NAME" "POSTGRES_HOST" "RABBITMQ_HOST")
  for var in "${required_vars[@]}"; do
      if [[ -z "${!var}" ]]; then
          echo "ОШИБКА: Не установлена обязательная переменная: $var" >&2
          return 1
      fi
  done

  # Проверяем, не занят ли порт
  if ss -tln | grep -q ":${new_port}\s"; then
      echo "ОШИБКА: Порт $new_port уже занят" >&2
      return 1
  fi

  # Проверяем, нет ли уже контейнера с таким именем
  if docker ps -a --filter "name=^/${container_name}$" --format "{{.Names}}" | grep -q "$container_name"; then
      echo "Удаление существующего контейнера $container_name..."
      docker stop "$container_name" 2>/dev/null
      docker rm "$container_name" 2>/dev/null
  fi

  # Запускаем новый контейнер
  echo "Запуск нового контейнера $container_name..."

  if ! docker run -d \
      --name "$container_name" \
      --restart always \
      --network infrastructure \
      --log-driver json-file \
      --log-opt max-size=10m \
      --log-opt max-file=3 \
      --health-cmd="curl -f http://localhost:8000/health || exit 1" \
      --health-interval=10s \
      --health-timeout=5s \
      --health-retries=3 \
      --memory="512m" \
      --cpus="0.5" \
      -v "/certs:/certs:ro" \
      -p "$new_port:8000" \
      -e PKPASS_PASSWORD="$PKPASS_PASSWORD" \
      -e APPLE_PASS_TYPE_ID="$APPLE_PASS_TYPE_ID" \
      -e APPLE_TEAM_ID="$APPLE_TEAM_ID" \
      -e APPLE_CERTIFICATE_PATH="$APPLE_CERTIFICATE_PATH" \
      -e APPLE_KEY_PATH="$APPLE_KEY_PATH" \
      -e APPLE_WWDR_PATH="$APPLE_WWDR_PATH" \
      -e APPLE_NOTIFICATION_PATH="$APPLE_NOTIFICATION_PATH" \
      -e APPLE_NOTIFICATION_KEY="$APPLE_NOTIFICATION_KEY" \
      -e RABBITMQ_HOST="$RABBITMQ_HOST" \
      -e RABBITMQ_PORT="$RABBITMQ_PORT" \
      -e RABBITMQ_USER="$RABBITMQ_USER" \
      -e RABBITMQ_PASS="$RABBITMQ_PASS" \
      -e RABBITMQ_VHOST="$RABBITMQ_VHOST" \
      -e APP_URL="$APP_URL" \
      -e S3_ACCESS="$S3_ACCESS" \
      -e S3_SECRET="$S3_SECRET" \
      -e S3_URL="$S3_URL" \
      -e S3_BACKUPS_ACCESSKEY="$S3_BACKUPS_ACCESSKEY" \
      -e S3_BACKUPS_SECRETKEY="$S3_BACKUPS_SECRETKEY" \
      -e TG_TOKEN="$TG_TOKEN" \
      -e POSTGRES_USER="$POSTGRES_USER" \
      -e POSTGRES_PASS="$POSTGRES_PASS" \
      -e POSTGRES_HOST="$POSTGRES_HOST" \
      -e POSTGRES_PORT="$POSTGRES_PORT" \
      -e CHEQUES_TOKEN="$CHEQUES_TOKEN" \
      -e ACCOUNT_INTERVAL="$ACCOUNT_INTERVAL" \
      -e ADMIN_ID="$ADMIN_ID" \
      -e YOOKASSA_OAUTH_APP_CLIENT_ID="$YOOKASSA_OAUTH_APP_CLIENT_ID" \
      -e YOOKASSA_OAUTH_APP_CLIENT_SECRET="$YOOKASSA_OAUTH_APP_CLIENT_SECRET" \
      -e RABBITMQ_USER_AMO_INTEGRATION="$RABBITMQ_USER_AMO_INTEGRATION" \
      -e RABBITMQ_PASS_AMO_INTEGRATION="$RABBITMQ_PASS_AMO_INTEGRATION" \
      -e RABBITMQ_HOST_AMO_INTEGRATION="$RABBITMQ_HOST_AMO_INTEGRATION" \
      -e RABBITMQ_PORT_AMO_INTEGRATION="$RABBITMQ_PORT_AMO_INTEGRATION" \
      -e RABBITMQ_VHOST_AMO_INTEGRATION="$RABBITMQ_VHOST_AMO_INTEGRATION" \
      -e GEOAPIFY_SECRET="$GEOAPIFY_SECRET" \
      "$IMAGE_NAME" \
      /bin/bash -c "uvicorn main:app --host=0.0.0.0 --port 8000 --log-level=info"; then

      echo "Контейнер $container_name успешно запущен"
  else
      echo "ОШИБКА: Не удалось запустить контейнер $container_name" >&2
      return 1
  fi

  # Ждем запуска сервиса с проверкой health-check
  echo "Ожидание запуска сервиса на порту $new_port..."
  local health_check_passed=false

  for ((i=1; i<=max_retries; i++)); do
      if curl --silent --fail --max-time 5 "http://localhost:${new_port}/health" >/dev/null 2>&1; then
          echo "Сервис на порту $new_port успешно запущен"
          health_check_passed=true
          break
      fi

      # Проверяем, что контейнер еще работает
      if ! docker ps --filter "name=^/${container_name}$" --filter "status=running" --quiet | grep -q .; then
          echo "Контейнер $container_name перестал работать. Проверьте логи: docker logs $container_name" >&2
          break
      fi

      echo "Попытка $i/$max_retries: Сервис еще не готов..."
      sleep "$retry_interval"
  done

  if [[ "$health_check_passed" != "true" ]]; then
      echo "ОШИБКА: Сервис на порту $new_port не запустился за отведенное время" >&2

      # Получаем логи упавшего контейнера
      echo "Последние 20 строк логов контейнера:"
      docker logs --tail 20 "$container_name" 2>&1

      # Откат
      echo "Откат: удаление контейнера $container_name..."
      docker stop "$container_name" 2>/dev/null
      docker rm "$container_name" 2>/dev/null

      return 1
  fi

  # Проверяем дополнительные эндпоинты если нужно
  echo "Проверка дополнительных эндпоинтов..."
  if curl --silent --fail --max-time 5 "http://localhost:${new_port}/api/v1/docs" >/dev/null 2>&1; then
      echo "Документация API доступна"
  fi

  # Обновляем конфигурацию nginx
  echo "Обновление конфигурации nginx..."
  if ! update_upstream_conf "$new_port"; then
      echo "ОШИБКА: Не удалось обновить конфигурацию nginx" >&2
      return 1
  fi

  # Перезагружаем nginx
  echo "Перезагрузка nginx..."
  if ! reload_nginx; then
      echo "ОШИБКА: Не удалось перезагрузить nginx" >&2
      return 1
  fi

  # Ждем стабилизации
  echo "Ожидание стабилизации сервиса (30 секунд)..."
  sleep 30

  # Проверяем, что новый сервис все еще работает
  if ! curl --silent --fail --max-time 5 "http://localhost:${new_port}/health" >/dev/null 2>&1; then
      echo "ОШИБКА: Новый сервис перестал отвечать после переключения" >&2
      return 1
  fi

  # Останавливаем старые контейнеры
  if [[ -n "$current_ports" ]]; then
      echo "Остановка старых контейнеров..."
      for port in $current_ports; do
          old_container="${SERVICE_NAME}_${port}"
          if [[ "$old_container" != "$container_name" ]] && \
              docker ps --filter "name=^/${old_container}$" --filter "status=running" --quiet | grep -q .; then
              echo "Останавливаем старый сервис: $old_container"
              docker stop "$old_container" && docker rm "$old_container" 2>/dev/null
          fi
      done
  fi

  # Очищаем остановленные контейнеры
  echo "Очистка остановленных контейнеров..."
  docker ps -a --filter "name=${SERVICE_NAME}" \
      --filter "status=exited" \
      --filter "status=dead" \
      --format '{{.Names}}' | \
      xargs --no-run-if-empty docker rm 2>/dev/null || true

  echo "=========================================="
  echo "Деплой завершен успешно!"
  echo "Сервис: $container_name"
  echo "Порт: $new_port"
  echo "Статус: $(docker ps --filter "name=^/${container_name}$" --format '{{.Status}}')"
  echo "Логи: docker logs $container_name"
  echo "=========================================="
}

deploy_new_bot_version() {
  current_bots=$(docker ps --filter "name=${BOT_SERVICE_NAME}" --format '{{.Names}}' | grep -E "${BOT_CONTAINER_NAME_PREFIX}_[0-9]+")

  if [[ -z "$current_bots" ]]; then
    NEW_BOT_NAME="${BOT_CONTAINER_NAME_PREFIX}_1"
  else
    last_bot=$(echo "$current_bots" | sort -V | tail -n1)
    last_num=$(echo "$last_bot" | grep -o '[0-9]\+$')
    NEW_NUM=$((last_num + 1))
    NEW_BOT_NAME="${BOT_CONTAINER_NAME_PREFIX}_${NEW_NUM}"
  fi

  echo "Деплой новой версии Telegram бота с именем $NEW_BOT_NAME"

  docker run -d \
    --name "$NEW_BOT_NAME" \
    --restart always \
    --network infrastructure \
    -v "/photos:/backend/photos" \
    -e RABBITMQ_HOST="$RABBITMQ_HOST" \
    -e RABBITMQ_PORT="$RABBITMQ_PORT" \
    -e RABBITMQ_USER="$RABBITMQ_USER" \
    -e RABBITMQ_PASS="$RABBITMQ_PASS" \
    -e RABBITMQ_VHOST="$RABBITMQ_VHOST" \
    -e APP_URL="$APP_URL" \
    -e S3_ACCESS="$S3_ACCESS" \
    -e S3_SECRET="$S3_SECRET" \
    -e S3_URL="$S3_URL" \
    -e S3_BACKUPS_ACCESSKEY="$S3_BACKUPS_ACCESSKEY" \
    -e S3_BACKUPS_SECRETKEY="$S3_BACKUPS_SECRETKEY" \
    -e TG_TOKEN="$TG_TOKEN" \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASS="$POSTGRES_PASS" \
    -e POSTGRES_HOST="$POSTGRES_HOST" \
    -e POSTGRES_PORT="$POSTGRES_PORT" \
    -e CHEQUES_TOKEN="$CHEQUES_TOKEN" \
    -e ACCOUNT_INTERVAL="$ACCOUNT_INTERVAL" \
    -e ADMIN_ID="$ADMIN_ID" \
    "$IMAGE_NAME" \
    /bin/bash -c "python3 bot.py"

  if [ -n "$current_bots" ]; then
    for bot in $current_bots; do
      echo "Останавливаем старый Telegram бот $bot"
      docker stop "$bot"
      docker rm "$bot"
    done
  fi

  echo "Деплой нового Telegram бота завершен успешно"
}

deploy_another_services() {
  docker stop "worker"
  docker rm "worker"

  docker run -d \
    --name "worker" \
    --restart always \
    --network infrastructure \
    -v "/certs:/certs" \
    -e PKPASS_PASSWORD="$PKPASS_PASSWORD" \
    -e APPLE_PASS_TYPE_ID="$APPLE_PASS_TYPE_ID" \
    -e APPLE_TEAM_ID="$APPLE_TEAM_ID" \
    -e APPLE_CERTIFICATE_PATH="$APPLE_CERTIFICATE_PATH" \
    -e APPLE_KEY_PATH="$APPLE_KEY_PATH" \
    -e APPLE_WWDR_PATH="$APPLE_WWDR_PATH" \
    -e APPLE_NOTIFICATION_PATH="$APPLE_NOTIFICATION_PATH" \
    -e APPLE_NOTIFICATION_KEY="$APPLE_NOTIFICATION_KEY" \
    -e RABBITMQ_HOST="$RABBITMQ_HOST" \
    -e RABBITMQ_PORT="$RABBITMQ_PORT" \
    -e RABBITMQ_USER="$RABBITMQ_USER" \
    -e RABBITMQ_PASS="$RABBITMQ_PASS" \
    -e RABBITMQ_VHOST="$RABBITMQ_VHOST" \
    -e APP_URL="$APP_URL" \
    -e S3_ACCESS="$S3_ACCESS" \
    -e S3_SECRET="$S3_SECRET" \
    -e S3_URL="$S3_URL" \
    -e S3_BACKUPS_ACCESSKEY="$S3_BACKUPS_ACCESSKEY" \
    -e S3_BACKUPS_SECRETKEY="$S3_BACKUPS_SECRETKEY" \
    -e TG_TOKEN="$TG_TOKEN" \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASS="$POSTGRES_PASS" \
    -e POSTGRES_HOST="$POSTGRES_HOST" \
    -e POSTGRES_PORT="$POSTGRES_PORT" \
    -e CHEQUES_TOKEN="$CHEQUES_TOKEN" \
    -e ACCOUNT_INTERVAL="$ACCOUNT_INTERVAL" \
    -e ADMIN_ID="$ADMIN_ID" \
    "$IMAGE_NAME" \
    /bin/bash -c "python3 worker.py"

  docker stop "backend_jobs"
  docker rm "backend_jobs"

  docker run -d \
    --name "backend_jobs" \
    --restart always \
    --network infrastructure \
    -v "/certs:/certs" \
    -e PKPASS_PASSWORD="$PKPASS_PASSWORD" \
    -e APPLE_PASS_TYPE_ID="$APPLE_PASS_TYPE_ID" \
    -e APPLE_TEAM_ID="$APPLE_TEAM_ID" \
    -e APPLE_CERTIFICATE_PATH="$APPLE_CERTIFICATE_PATH" \
    -e APPLE_KEY_PATH="$APPLE_KEY_PATH" \
    -e APPLE_WWDR_PATH="$APPLE_WWDR_PATH" \
    -e APPLE_NOTIFICATION_PATH="$APPLE_NOTIFICATION_PATH" \
    -e APPLE_NOTIFICATION_KEY="$APPLE_NOTIFICATION_KEY" \
    -e RABBITMQ_HOST="$RABBITMQ_HOST" \
    -e RABBITMQ_PORT="$RABBITMQ_PORT" \
    -e RABBITMQ_USER="$RABBITMQ_USER" \
    -e RABBITMQ_PASS="$RABBITMQ_PASS" \
    -e RABBITMQ_VHOST="$RABBITMQ_VHOST" \
    -e APP_URL="$APP_URL" \
    -e S3_ACCESS="$S3_ACCESS" \
    -e S3_SECRET="$S3_SECRET" \
    -e S3_URL="$S3_URL" \
    -e S3_BACKUPS_ACCESSKEY="$S3_BACKUPS_ACCESSKEY" \
    -e S3_BACKUPS_SECRETKEY="$S3_BACKUPS_SECRETKEY" \
    -e TG_TOKEN="$TG_TOKEN" \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASS="$POSTGRES_PASS" \
    -e POSTGRES_HOST="$POSTGRES_HOST" \
    -e POSTGRES_PORT="$POSTGRES_PORT" \
    -e CHEQUES_TOKEN="$CHEQUES_TOKEN" \
    -e ACCOUNT_INTERVAL="$ACCOUNT_INTERVAL" \
    -e ADMIN_ID="$ADMIN_ID" \
    "$IMAGE_NAME" \
    /bin/bash -c "python3 start_jobs.py"

  docker stop "message_consumer_task"
  docker rm "message_consumer_task"

  docker run -d \
    --name "message_consumer_task" \
    --restart always \
    --network infrastructure \
    -e PKPASS_PASSWORD="$PKPASS_PASSWORD" \
    -e APPLE_PASS_TYPE_ID="$APPLE_PASS_TYPE_ID" \
    -e APPLE_TEAM_ID="$APPLE_TEAM_ID" \
    -e APPLE_CERTIFICATE_PATH="$APPLE_CERTIFICATE_PATH" \
    -e APPLE_KEY_PATH="$APPLE_KEY_PATH" \
    -e APPLE_WWDR_PATH="$APPLE_WWDR_PATH" \
    -e APPLE_NOTIFICATION_PATH="$APPLE_NOTIFICATION_PATH" \
    -e APPLE_NOTIFICATION_KEY="$APPLE_NOTIFICATION_KEY" \
    -e RABBITMQ_HOST="$RABBITMQ_HOST" \
    -e RABBITMQ_PORT="$RABBITMQ_PORT" \
    -e RABBITMQ_USER="$RABBITMQ_USER" \
    -e RABBITMQ_PASS="$RABBITMQ_PASS" \
    -e RABBITMQ_VHOST="$RABBITMQ_VHOST" \
    -e APP_URL="$APP_URL" \
    -e S3_ACCESS="$S3_ACCESS" \
    -e S3_SECRET="$S3_SECRET" \
    -e S3_URL="$S3_URL" \
    -e S3_BACKUPS_ACCESSKEY="$S3_BACKUPS_ACCESSKEY" \
    -e S3_BACKUPS_SECRETKEY="$S3_BACKUPS_SECRETKEY" \
    -e TG_TOKEN="$TG_TOKEN" \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASS="$POSTGRES_PASS" \
    -e POSTGRES_HOST="$POSTGRES_HOST" \
    -e POSTGRES_PORT="$POSTGRES_PORT" \
    -e CHEQUES_TOKEN="$CHEQUES_TOKEN" \
    -e ACCOUNT_INTERVAL="$ACCOUNT_INTERVAL" \
    -e ADMIN_ID="$ADMIN_ID" \
    "$IMAGE_NAME" \
    /bin/bash -c "python3 message_consumer.py"

  docker stop "notification_consumer_task"
  docker rm "notification_consumer_task"

  docker run -d \
    --name "notification_consumer_task" \
    --restart always \
    --network infrastructure \
    -e PKPASS_PASSWORD="$PKPASS_PASSWORD" \
    -e APPLE_PASS_TYPE_ID="$APPLE_PASS_TYPE_ID" \
    -e APPLE_TEAM_ID="$APPLE_TEAM_ID" \
    -e APPLE_CERTIFICATE_PATH="$APPLE_CERTIFICATE_PATH" \
    -e APPLE_KEY_PATH="$APPLE_KEY_PATH" \
    -e APPLE_WWDR_PATH="$APPLE_WWDR_PATH" \
    -e APPLE_NOTIFICATION_PATH="$APPLE_NOTIFICATION_PATH" \
    -e APPLE_NOTIFICATION_KEY="$APPLE_NOTIFICATION_KEY" \
    -e RABBITMQ_HOST="$RABBITMQ_HOST" \
    -e RABBITMQ_PORT="$RABBITMQ_PORT" \
    -e RABBITMQ_USER="$RABBITMQ_USER" \
    -e RABBITMQ_PASS="$RABBITMQ_PASS" \
    -e RABBITMQ_VHOST="$RABBITMQ_VHOST" \
    -e APP_URL="$APP_URL" \
    -e S3_ACCESS="$S3_ACCESS" \
    -e S3_SECRET="$S3_SECRET" \
    -e S3_URL="$S3_URL" \
    -e S3_BACKUPS_ACCESSKEY="$S3_BACKUPS_ACCESSKEY" \
    -e S3_BACKUPS_SECRETKEY="$S3_BACKUPS_SECRETKEY" \
    -e TG_TOKEN="$TG_TOKEN" \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASS="$POSTGRES_PASS" \
    -e POSTGRES_HOST="$POSTGRES_HOST" \
    -e POSTGRES_PORT="$POSTGRES_PORT" \
    -e CHEQUES_TOKEN="$CHEQUES_TOKEN" \
    -e ACCOUNT_INTERVAL="$ACCOUNT_INTERVAL" \
    -e ADMIN_ID="$ADMIN_ID" \
    "$IMAGE_NAME" \
    /bin/bash -c "python3 notification_consumer.py"
}

deploy_new_version
deploy_new_bot_version
deploy_another_services
