#!/bin/bash

BRANCH_NAME=$1
NGINX_CONF_PATH="/etc/nginx/nginx.conf"

echo $(pwd)

if [ "$BRANCH_NAME" = "dev" ]; then
    cp nginx.dev.conf $NGINX_CONF_PATH
else
    cp nginx.conf $NGINX_CONF_PATH
fi