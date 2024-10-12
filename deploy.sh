SERVICE_NAME="backend"
IMAGE_NAME="git.tablecrm.com:5050/tablecrm/tablecrm/backend:$CI_COMMIT_SHA"
NGINX_CONTAINER_NAME="tablecrm-nginx-1"
UPSTREAM_CONF_PATH="/etc/nginx/dir/upstream.conf"

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

deploy_new_version