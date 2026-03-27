#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: bash scripts/load-db.sh path/to/file.sql"
  exit 1
fi

INPUT_PATH="$1"

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
  powermoon_inventory < "$SQL_FILE"

echo "Imported $SQL_FILE into powermoon_inventory"