#! /bin/bash

set -eu
set -o pipefail

source ./env.sh

echo "Creating backup of $POSTGRES_DATABASE database..."
pg_dump --format=plain \
        -h $POSTGRES_HOST \
        -p $POSTGRES_PORT \
        -U $POSTGRES_USER \
        -d $POSTGRES_DATABASE \
        $PGDUMP_EXTRA_OPTS \
        > db.sql

timestamp=$(date +"%Y-%m-%dT%H:%M:%S")
s3_uri_base="s3://${S3_BUCKET}/${S3_PREFIX}/${POSTGRES_DATABASE}_${timestamp}.sql"

if [ -n "$PASSPHRASE" ]; then
  echo "Encrypting backup..."
  gpg --symmetric --batch --passphrase "$PASSPHRASE" db.sql
  rm db.sql
  local_file="db.sql.gpg"
  s3_uri="${s3_uri_base}.gpg"
else
  local_file="db.sql"
  s3_uri="$s3_uri_base"
fi

echo "Uploading backup to $S3_BUCKET..."
aws $aws_args s3 cp "$local_file" "$s3_uri"
rm "$local_file"

echo "Backup complete."

sec=$((86400*${BACKUP_KEEP_DAYS:-14}))
date_from_remove=$(date -d "@$(($(date +%s) - sec))" +%Y-%m-%d)
previous_day=$(date -d "yesterday" +"%Y-%m-%d")
backups_query="Contents[?((LastModified<'${date_from_remove}T00:00:00Z') || (LastModified>='${previous_day}T00:00:00Z' && LastModified<'${previous_day}T22:01:00Z'))].{Key: Key}"

echo "Removing old backups from $S3_BUCKET..."
aws $aws_args s3api list-objects \
  --bucket "${S3_BUCKET}" \
  --prefix "${S3_PREFIX}" \
  --query "${backups_query}" \
  --output text \
  | xargs --no-run-if-empty -t -I 'KEY' aws $aws_args s3 rm s3://"${S3_BUCKET}"/'KEY'
echo "Removal complete."
