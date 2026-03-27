# Inventory Webapp

Dockerized Node.js + MariaDB inventory application.

## Stack

- Backend: Node.js, Express, mysql2
- Database: MariaDB (Docker)
- Frontend: static files served by Express

## Additional Documentation

- Project change summary: `PROJECT_CHANGES.md`

## Quick Start

0. Create local environment files if they do not exist yet:

```bash
cp .env.example .env
cp server/.env.example server/.env
```

Adjust the DB host/port in `server/.env` if you run the backend directly outside Docker.

1. Build and start services:

```bash
docker compose up -d --build
```

2. Open the app:

- http://localhost:5555/

3. Stop services:

```bash
docker compose stop
```

4. Stop and remove containers (keep DB data volume):

```bash
docker compose down
```

5. Stop and remove containers + delete DB data volume:

```bash
docker compose down -v
```

## Database Behavior

- The DB data is persisted in the named Docker volume `db_data`.
- On the first startup of a fresh DB volume, MariaDB automatically runs `db/init/01-schema.sql` to create the core inventory tables.
- The app then bootstraps auxiliary tables on startup, such as `users`, `app_logs`, `production_runs`, and `production_run_materials`.
- No automatic data reseeding runs at startup.
- This means normal restarts keep all edited data.
- `replace-db.sh` is only needed when you want to restore an existing dataset or backup, not to make a fresh install usable.

## Backup and Restore

### 1) Create a backup (script)

Creates a timestamped SQL dump in `backups/`.

```bash
bash scripts/backup-db.sh
```

Optional custom output folder:

```bash
bash scripts/backup-db.sh ./my-backups
```

### 2) Create a backup (compose tools profile)

Runs the dedicated one-off backup service.

```bash
docker compose --profile tools run --rm db-backup
```

### 3) Import a SQL file into the existing DB (non-destructive)

Loads SQL into the current `powermoon_inventory` database.

```bash
bash scripts/load-db.sh path/to/file.sql
```

### 4) Replace DB with a SQL file (destructive)

Drops and recreates the target DB, then imports SQL.

```bash
bash scripts/replace-db.sh --yes path/to/file.sql
```

Restore into a different database name:

```bash
bash scripts/replace-db.sh --yes path/to/file.sql some_other_db
```

Use this when you want to restore real data from a backup. You do not need it for a brand-new machine anymore, because the base schema now boots automatically on a fresh DB volume.

## Useful Checks

Show running services:

```bash
docker compose ps
```

Tail app logs:

```bash
docker compose logs -f app
```

Tail DB logs:

```bash
docker compose logs -f db
```

List DB tables:

```bash
docker compose exec -T db sh -lc 'mariadb -uroot -p"$MARIADB_ROOT_PASSWORD" -e "USE $MARIADB_DATABASE; SHOW TABLES;"'
```

## Automated Tests

Tests live under `server/tests/`.

### Unit tests

Run the fast auth/session unit tests:

```bash
cd server
npm test
```

This covers:

- password hashing and verification
- session token creation and validation
- session expiry handling
- session cookie helpers

### Smoke tests

Run the API smoke tests against the live app:

```bash
cd server
npm run test:smoke
```

This covers:

- login, logout, and `auth/me`
- role permissions for `admin` vs `powermoon`
- stock movements
- finished products
- production planner
- PDF and CSV exports

Requirements:

- the app must already be running at `http://localhost:5555/`
- if your local credentials differ, pass them explicitly with the environment variables below
- if the database was freshly seeded, the temporary seed passwords come from `.env` / `server/.env`
- seeded users are forced to change their password on the first login, so explicit credentials are recommended

Optional overrides:

```bash
SMOKE_TEST_BASE_URL=http://localhost:5555 npm run test:smoke
SMOKE_TEST_ADMIN_USERNAME=admin SMOKE_TEST_ADMIN_PASSWORD=your-admin-password npm run test:smoke
SMOKE_TEST_POWERMOON_USERNAME=powermoon SMOKE_TEST_POWERMOON_PASSWORD=your-powermoon-password npm run test:smoke
```

Smoke test commands (copy/paste):

```bash
docker compose up -d --build

cd server
npm test

SMOKE_TEST_ADMIN_USERNAME=admin \
SMOKE_TEST_ADMIN_PASSWORD=your-admin-password \
SMOKE_TEST_POWERMOON_USERNAME=powermoon \
SMOKE_TEST_POWERMOON_PASSWORD=your-standard-user-password \
npm run test:smoke
```

Run everything:

```bash
cd server
npm run test:all
```

Notes:

- smoke tests create temporary products, parts, stock movements, and finished products
- they clean up those temporary records automatically at the end of the run

## Notes

- If backups are important, keep timestamped files and avoid overwriting a single dump file.
- For production-like setups, use external secret management instead of plain-text passwords in compose files.

## Common Recovery Playbook

Use this when the app is up but pages fail or DB errors appear.

1. Rebuild and restart the stack:

```bash
docker compose up -d --build
```

2. Verify service health and recent logs:

```bash
docker compose ps
docker compose logs --tail=120 db
docker compose logs --tail=120 app
```

3. Validate data availability and test one API endpoint:

```bash
docker compose exec -T db sh -lc 'mariadb -uroot -p"$MARIADB_ROOT_PASSWORD" -e "USE $MARIADB_DATABASE; SHOW TABLES;"'
curl -sS http://localhost:5555/parts | head -c 300
```

If tables are missing, restore from backup:

```bash
bash scripts/load-db.sh backups/your-backup.sql
```

If you need a full reset from backup (destructive):

```bash
bash scripts/replace-db.sh --yes backups/your-backup.sql
```

## Troubleshooting

### App starts but DB queries fail

Check service health first:

```bash
docker compose ps
```

If `db` is not healthy, inspect DB startup logs:

```bash
docker compose logs --tail=200 db
```

### `ECONNREFUSED` to `db:3306`

- `db:3306` is the correct internal Docker network target.
- `depends_on` waits for DB health, but DB can still be briefly unavailable during restarts.

Check app logs for fresh connection errors:

```bash
docker compose logs --tail=200 app
```

### `ER_NO_SUCH_TABLE` errors

The app connected, but schema/data is missing in the target DB.

Verify tables:

```bash
docker compose exec -T db sh -lc 'mariadb -uroot -p"$MARIADB_ROOT_PASSWORD" -e "USE $MARIADB_DATABASE; SHOW TABLES;"'
```

Restore a backup into the existing DB:

```bash
bash scripts/load-db.sh backups/your-backup.sql
```

Or replace DB completely (destructive):

```bash
bash scripts/replace-db.sh --yes backups/your-backup.sql
```

### Why init SQL does not run again

Docker init scripts only run on a fresh DB data directory. If `db_data` already exists, MariaDB skips initialization scripts.

If you intentionally want a fresh DB directory:

```bash
docker compose down -v
docker compose up -d --build
```

Warning: `down -v` deletes persisted DB data.

### Verify active DB connection settings

The backend resolves connection values from env vars in this order:

- Host: `DB_HOST` then `HOST`
- User: `DB_USER` then `USERS`
- Password: `DB_PASSWORD` then `PASSWORD`
- Database: `DB_NAME` then `DATABASE`
- Port: `DB_PORT` then `3306`

Check what Compose injects into `app`:

```bash
docker compose config
```
