SERVICE_NAME="backend"
IMAGE_NAME="git.tablecrm.com:5050/tablecrm/tablecrm/backend:$CI_COMMIT_SHA"
NGINX_CONTAINER_NAME="tablecrm-nginx-1"
UPSTREAM_CONF_PATH="/etc/nginx/dir/upstream.conf"
BOT_SERVICE_NAME="telegram_bot"
BOT_CONTAINER_NAME_PREFIX="telegram_bot"

reload_nginx() {
  docker exec $NGINX_CONTAINER_NAME nginx -s reload
}

update_upstream_conf() {
  NEW_PORT=$1

  new_container_id=$(docker ps -f name="${SERVICE_NAME}_$NEW_PORT" -q | head -n1)
  new_container_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{"\n"}}{{end}}' $new_container_id | head -n1)

  echo "Обновление upstream.conf для указания порта $NEW_PORT и ip ${new_container_ip}"

  echo "server ${new_container_ip}:8000;" > upstream.conf.tmp

  docker cp upstream.conf.tmp $NGINX_CONTAINER_NAME:$UPSTREAM_CONF_PATH

  rm upstream.conf.tmp
}

deploy_new_version() {
  current_ports=$(docker ps --filter "name=$SERVICE_NAME" --format '{{.Ports}}' | grep -o '0\.0\.0\.0:[0-9]\+' | grep -o '[0-9]\+')

  if [[ " $current_ports " =~ "8000" ]]; then
    NEW_PORT=8002
  else
    NEW_PORT=8000
  fi

  echo "Деплой новой версии на порт $NEW_PORT"


  docker run -d \
    --name "${SERVICE_NAME}_$NEW_PORT" \
    --network infrastructure \
    -p $NEW_PORT:8000 \
    -e RABBITMQ_HOST=$RABBITMQ_HOST \
    -e RABBITMQ_PORT=$RABBITMQ_PORT \
    -e RABBITMQ_USER=$RABBITMQ_USER \
    -e RABBITMQ_PASS=$RABBITMQ_PASS \
    -e RABBITMQ_VHOST=$RABBITMQ_VHOST \
    -e APP_URL=$APP_URL \
    -e S3_ACCESS=$S3_ACCESS \
    -e S3_SECRET=$S3_SECRET \
    -e S3_URL=$S3_URL \
    -e S3_BACKUPS_ACCESSKEY=$S3_BACKUPS_ACCESSKEY \
    -e S3_BACKUPS_SECRETKEY=$S3_BACKUPS_SECRETKEY \
    -e TG_TOKEN=$TG_TOKEN \
    -e POSTGRES_USER=$POSTGRES_USER \
    -e POSTGRES_PASS=$POSTGRES_PASS \
    -e POSTGRES_HOST=$POSTGRES_HOST \
    -e POSTGRES_PORT=$POSTGRES_PORT \
    -e CHEQUES_TOKEN=$CHEQUES_TOKEN \
    -e ACCOUNT_INTERVAL=$ACCOUNT_INTERVAL \
    -e ACCOUNT_INTERVAL=$ACCOUNT_INTERVAL \
    $IMAGE_NAME \
    /bin/bash -c "uvicorn main:app --host=0.0.0.0 --port 8000 --log-level=info"

  for i in {1..30}; do
    if curl --silent --fail http://localhost:${NEW_PORT}/health; then
      echo "Новый сервис на порту $NEW_PORT успешно запущен"
      break
    fi
    echo "Ожидание запуска сервиса на порту $NEW_PORT..."
    sleep 1
  done

  if ! curl --silent --fail http://localhost:${NEW_PORT}/health; then
    echo "Новый сервис на порту $NEW_PORT не отвечает. Откат."
    docker stop "${SERVICE_NAME}_$NEW_PORT"
    docker rm "${SERVICE_NAME}_$NEW_PORT"
    exit 1
  fi

  update_upstream_conf $NEW_PORT

  reload_nginx

  sleep 30

  if [ -n "$current_ports" ]; then
    for port in $current_ports; do
      echo "Останавливаем старый сервис на порту $port"
      docker stop "${SERVICE_NAME}_${port}"
      docker rm "${SERVICE_NAME}_${port}"
    done
  fi

  echo "Деплой завершен успешно"
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
    --network infrastructure \
    -v "/photos:/backend/photos" \
    -e RABBITMQ_HOST=$RABBITMQ_HOST \
    -e RABBITMQ_PORT=$RABBITMQ_PORT \
    -e RABBITMQ_USER=$RABBITMQ_USER \
    -e RABBITMQ_PASS=$RABBITMQ_PASS \
    -e RABBITMQ_VHOST=$RABBITMQ_VHOST \
    -e APP_URL=$APP_URL \
    -e S3_ACCESS=$S3_ACCESS \
    -e S3_SECRET=$S3_SECRET \
    -e S3_URL=$S3_URL \
    -e S3_BACKUPS_ACCESSKEY=$S3_BACKUPS_ACCESSKEY \
    -e S3_BACKUPS_SECRETKEY=$S3_BACKUPS_SECRETKEY \
    -e TG_TOKEN=$TG_TOKEN \
    -e POSTGRES_USER=$POSTGRES_USER \
    -e POSTGRES_PASS=$POSTGRES_PASS \
    -e POSTGRES_HOST=$POSTGRES_HOST \
    -e POSTGRES_PORT=$POSTGRES_PORT \
    -e CHEQUES_TOKEN=$CHEQUES_TOKEN \
    -e ACCOUNT_INTERVAL=$ACCOUNT_INTERVAL \
    $IMAGE_NAME \
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
  docker stop "repeat_worker"
  docker rm "repeat_worker"

  docker run -d \
    --name "repeat_worker" \
    --network infrastructure \
    -e RABBITMQ_HOST=$RABBITMQ_HOST \
    -e RABBITMQ_PORT=$RABBITMQ_PORT \
    -e RABBITMQ_USER=$RABBITMQ_USER \
    -e RABBITMQ_PASS=$RABBITMQ_PASS \
    -e RABBITMQ_VHOST=$RABBITMQ_VHOST \
    -e APP_URL=$APP_URL \
    -e S3_ACCESS=$S3_ACCESS \
    -e S3_SECRET=$S3_SECRET \
    -e S3_URL=$S3_URL \
    -e S3_BACKUPS_ACCESSKEY=$S3_BACKUPS_ACCESSKEY \
    -e S3_BACKUPS_SECRETKEY=$S3_BACKUPS_SECRETKEY \
    -e TG_TOKEN=$TG_TOKEN \
    -e POSTGRES_USER=$POSTGRES_USER \
    -e POSTGRES_PASS=$POSTGRES_PASS \
    -e POSTGRES_HOST=$POSTGRES_HOST \
    -e POSTGRES_PORT=$POSTGRES_PORT \
    -e CHEQUES_TOKEN=$CHEQUES_TOKEN \
    -e ACCOUNT_INTERVAL=$ACCOUNT_INTERVAL \
    $IMAGE_NAME \
    /bin/bash -c "python3 run_repeat_worker.py"

  docker stop "post_amo_lead_worker"
  docker rm "post_amo_lead_worker"

  docker run -d \
    --name "post_amo_lead_worker" \
    --network infrastructure \
    -e RABBITMQ_HOST=$RABBITMQ_HOST \
    -e RABBITMQ_PORT=$RABBITMQ_PORT \
    -e RABBITMQ_USER=$RABBITMQ_USER \
    -e RABBITMQ_PASS=$RABBITMQ_PASS \
    -e RABBITMQ_VHOST=$RABBITMQ_VHOST \
    -e APP_URL=$APP_URL \
    -e S3_ACCESS=$S3_ACCESS \
    -e S3_SECRET=$S3_SECRET \
    -e S3_URL=$S3_URL \
    -e S3_BACKUPS_ACCESSKEY=$S3_BACKUPS_ACCESSKEY \
    -e S3_BACKUPS_SECRETKEY=$S3_BACKUPS_SECRETKEY \
    -e TG_TOKEN=$TG_TOKEN \
    -e POSTGRES_USER=$POSTGRES_USER \
    -e POSTGRES_PASS=$POSTGRES_PASS \
    -e POSTGRES_HOST=$POSTGRES_HOST \
    -e POSTGRES_PORT=$POSTGRES_PORT \
    -e CHEQUES_TOKEN=$CHEQUES_TOKEN \
    -e ACCOUNT_INTERVAL=$ACCOUNT_INTERVAL \
    $IMAGE_NAME \
    /bin/bash -c "python3 run_amo_post_worker.py"

  docker stop "backend_jobs"
  docker rm "backend_jobs"

  docker run -d \
    --name "backend_jobs" \
    --network infrastructure \
    -e RABBITMQ_HOST=$RABBITMQ_HOST \
    -e RABBITMQ_PORT=$RABBITMQ_PORT \
    -e RABBITMQ_USER=$RABBITMQ_USER \
    -e RABBITMQ_PASS=$RABBITMQ_PASS \
    -e RABBITMQ_VHOST=$RABBITMQ_VHOST \
    -e APP_URL=$APP_URL \
    -e S3_ACCESS=$S3_ACCESS \
    -e S3_SECRET=$S3_SECRET \
    -e S3_URL=$S3_URL \
    -e S3_BACKUPS_ACCESSKEY=$S3_BACKUPS_ACCESSKEY \
    -e S3_BACKUPS_SECRETKEY=$S3_BACKUPS_SECRETKEY \
    -e TG_TOKEN=$TG_TOKEN \
    -e POSTGRES_USER=$POSTGRES_USER \
    -e POSTGRES_PASS=$POSTGRES_PASS \
    -e POSTGRES_HOST=$POSTGRES_HOST \
    -e POSTGRES_PORT=$POSTGRES_PORT \
    -e CHEQUES_TOKEN=$CHEQUES_TOKEN \
    -e ACCOUNT_INTERVAL=$ACCOUNT_INTERVAL \
    $IMAGE_NAME \
    /bin/bash -c "python3 start_jobs.py"

  docker stop "message_consumer_task"
  docker rm "message_consumer_task"

  docker run -d \
    --name "message_consumer_task" \
    --network infrastructure \
    -e RABBITMQ_HOST=$RABBITMQ_HOST \
    -e RABBITMQ_PORT=$RABBITMQ_PORT \
    -e RABBITMQ_USER=$RABBITMQ_USER \
    -e RABBITMQ_PASS=$RABBITMQ_PASS \
    -e RABBITMQ_VHOST=$RABBITMQ_VHOST \
    -e APP_URL=$APP_URL \
    -e S3_ACCESS=$S3_ACCESS \
    -e S3_SECRET=$S3_SECRET \
    -e S3_URL=$S3_URL \
    -e S3_BACKUPS_ACCESSKEY=$S3_BACKUPS_ACCESSKEY \
    -e S3_BACKUPS_SECRETKEY=$S3_BACKUPS_SECRETKEY \
    -e TG_TOKEN=$TG_TOKEN \
    -e POSTGRES_USER=$POSTGRES_USER \
    -e POSTGRES_PASS=$POSTGRES_PASS \
    -e POSTGRES_HOST=$POSTGRES_HOST \
    -e POSTGRES_PORT=$POSTGRES_PORT \
    -e CHEQUES_TOKEN=$CHEQUES_TOKEN \
    -e ACCOUNT_INTERVAL=$ACCOUNT_INTERVAL \
    $IMAGE_NAME \
    /bin/bash -c "python3 message_consumer.py"
}

deploy_new_version
deploy_new_bot_version
deploy_another_services