#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKUP_DIR="${1:-$ROOT_DIR/backups}"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
OUTPUT_FILE="$BACKUP_DIR/powermoon_inventory-$TIMESTAMP.sql"

mkdir -p "$BACKUP_DIR"

cd "$ROOT_DIR"
docker compose exec -T db mariadb-dump \
  -uroot \
  -ppowermoon26 \
  --single-transaction \
  --quick \
  --skip-lock-tables \
  powermoon_inventory > "$OUTPUT_FILE"

echo "Backup written to $OUTPUT_FILE"