#!/usr/bin/env bash

set -euo pipefail

CONFIRMED="false"
POSITIONAL_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --yes)
      CONFIRMED="true"
      shift
      ;;
    *)
      POSITIONAL_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ ${#POSITIONAL_ARGS[@]} -lt 1 ]]; then
  echo "Usage: bash scripts/replace-db.sh --yes path/to/file.sql [database_name]"
  exit 1
fi

if [[ "$CONFIRMED" != "true" ]]; then
  echo "Refusing to replace the database without --yes"
  exit 1
fi

INPUT_PATH="${POSITIONAL_ARGS[0]}"
TARGET_DB="${POSITIONAL_ARGS[1]:-powermoon_inventory}"

if [[ ! "$TARGET_DB" =~ ^[A-Za-z0-9_]+$ ]]; then
  echo "Invalid database name: $TARGET_DB"
  exit 1
fi

if [[ "$INPUT_PATH" = /* ]]; then
  SQL_FILE="$INPUT_PATH"
else
  SQL_FILE="$(pwd)/$INPUT_PATH"
fi

if [[ ! -f "$SQL_FILE" ]]; then
  echo "SQL file not found: $SQL_FILE"
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"
docker compose exec -T db mariadb \
  -uroot \
  -ppowermoon26 \
  -e "DROP DATABASE IF EXISTS $TARGET_DB; CREATE DATABASE $TARGET_DB;"

docker compose exec -T db mariadb \
  -uroot \
  -ppowermoon26 \
  "$TARGET_DB" < "$SQL_FILE"

echo "Replaced database $TARGET_DB using $SQL_FILE"