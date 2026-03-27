// ============================================================================
// Database Service
// ============================================================================
// Purpose:
// - Centralize SQL access in one class so route handlers stay concise.
// - Keep schema-variant handling (column name differences) in one place.
// - Provide transaction-safe writes for multi-table operations.
//
// File map:
// 1) Bootstrap and pool setup
// 2) Internal execution/transaction helpers
// 3) Schema discovery and index bootstrapping
// 4) Parts + BOM management
// 5) Products and buildability
// 6) Finished products and stock movement history
// 7) Current stock snapshot query
// ============================================================================
const mysql = require('mysql2');
const dotenv = require('dotenv');
const path = require('path');
let instance = null;
dotenv.config({ path: path.join(__dirname, '.env') });

// Shared connection pool for the full process lifetime.
const pool = mysql.createPool({
    host: process.env.DB_HOST || process.env.HOST,
    user: process.env.DB_USER || process.env.USERS || 'powermoon',
    password: process.env.DB_PASSWORD || process.env.PASSWORD,
    database: process.env.DB_NAME || process.env.DATABASE,
    port: Number(process.env.DB_PORT || 3306),
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});
const promisePool = pool.promise();

// Singleton service used by server routes.
class DbService {
    // Tracks one-time setup work and schema-discovery caches.
    constructor() {
        this._indexesEnsured = false;
        this._authSchemaEnsured = false;
        this._appLogSchemaEnsured = false;
        this._productionSnapshotSchemaEnsured = false;
        this._productionScheduleSchemaEnsured = false;
        this._partColumnSetCache = null;
        this._tricomaColumnCache = null;
        this._bomQuantityColumnCache = null;
    }

    // Expose one shared service instance so the whole app uses the same pool,
    // caches, and schema-discovery results for the process lifetime.
    static getDbServiceInstance() {
        if (!instance) {
            instance = new DbService();
        }
        return instance;
    }

    // ------------------------------------------------------------------------
    // Section 1: Internal SQL Execution Helpers
    // ------------------------------------------------------------------------
    // Executes one SQL statement through the shared pool.
    // Returns only the first tuple entry (`results`) for simpler call sites.
    _exec(query, params = []) {
        return promisePool
            .query(query, params)
            .then(([results]) => results)
            .catch((err) => {
                throw new Error(err.message);
            });
    }

    // Executes one SQL statement on a caller-provided transactional connection.
    _execOnConnection(connection, query, params = []) {
        return connection
            .query(query, params)
            .then(([results]) => results)
            .catch((err) => {
                throw new Error(err.message);
            });
    }

    // Runs a callback inside BEGIN/COMMIT with rollback on any thrown error.
    // Always releases the connection back to the pool in the `finally` block.
    async _withTransaction(work) {
        const connection = await promisePool.getConnection();
        try {
            await connection.beginTransaction();
            const result = await work(connection);
            await connection.commit();
            return result;
        } catch (error) {
            await connection.rollback();
            throw error;
        } finally {
            connection.release();
        }
    }

    // ------------------------------------------------------------------------
    // Section 2: Schema Discovery and Bootstrap
    // ------------------------------------------------------------------------
    // Reads and caches `parts` table columns so schema checks are not repeated.
    async _getPartColumnSet() {
        if (this._partColumnSetCache) {
            return this._partColumnSetCache;
        }

        const partColumns = await this._exec('SHOW COLUMNS FROM parts;');
        this._partColumnSetCache = new Set(partColumns.map(({ Field }) => String(Field).toLowerCase()));
        return this._partColumnSetCache;
    }

    async _tableHasColumn(tableName, columnName) {
        const rows = await this._exec(
            `
                SELECT 1
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                  AND COLUMN_NAME = ?
                LIMIT 1;
            `,
            [tableName, columnName]
        );

        return Array.isArray(rows) && rows.length > 0;
    }

    async _tableHasIndex(tableName, indexName) {
        const rows = await this._exec(
            `
                SELECT 1
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                  AND INDEX_NAME = ?
                LIMIT 1;
            `,
            [tableName, indexName]
        );

        return Array.isArray(rows) && rows.length > 0;
    }

    async _tableHasConstraint(tableName, constraintName) {
        const rows = await this._exec(
            `
                SELECT 1
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = ?
                  AND CONSTRAINT_NAME = ?
                LIMIT 1;
            `,
            [tableName, constraintName]
        );

        return Array.isArray(rows) && rows.length > 0;
    }

    // Ensures common lookup/join indexes exist.
    // Safe to call on startup: existing indexes are detected and skipped.
    async ensurePerformanceIndexes() {
        if (this._indexesEnsured) return;

        const desiredIndexes = [
            { table: 'bom', index: 'idx_bom_product_id', columns: ['product_id'] },
            { table: 'bom', index: 'idx_bom_part_id', columns: ['part_id'] },
            { table: 'stock_movement', index: 'idx_stock_movement_part_id', columns: ['part_id'] },
            { table: 'stock_movement', index: 'idx_stock_movement_date', columns: ['stock_movement_date'] },
            { table: 'finished_products', index: 'idx_finished_product_product_id', columns: ['product_id'] },
            { table: 'finished_products', index: 'idx_finished_product_date', columns: ['fin_product_date'] },
            { table: 'products', index: 'idx_products_product_name', columns: ['product_name'] },
            { table: 'parts', index: 'idx_parts_part_name', columns: ['part_name'] }
        ];

        for (const spec of desiredIndexes) {
            const existing = await this._exec(
                `
                    SELECT 1
                    FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = ?
                      AND INDEX_NAME = ?
                    LIMIT 1;
                `,
                [spec.table, spec.index]
            );

            if (Array.isArray(existing) && existing.length > 0) {
                continue;
            }

            const escapedColumns = spec.columns.map((column) => mysql.escapeId(column)).join(', ');
            await this._exec(
                `CREATE INDEX ${mysql.escapeId(spec.index)} ON ${mysql.escapeId(spec.table)} (${escapedColumns});`
            );
        }

        this._indexesEnsured = true;
    }

    // Adds the authentication tables and audit columns used by login/session-aware
    // inventory changes. The method is idempotent so it can safely run on startup.
    async ensureAuthSchema() {
        if (this._authSchemaEnsured) return;

        await this._exec(
            `
                CREATE TABLE IF NOT EXISTS users (
                    user_id INT NOT NULL AUTO_INCREMENT,
                    username VARCHAR(60) NOT NULL,
                    display_name VARCHAR(120) NOT NULL,
                    role VARCHAR(20) NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    session_version INT NOT NULL DEFAULT 1,
                    must_change_password TINYINT(1) NOT NULL DEFAULT 0,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id),
                    UNIQUE KEY uq_users_username (username),
                    KEY idx_users_role (role)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
            `
        );

        const hasMustChangePasswordColumn = await this._tableHasColumn('users', 'must_change_password');
        if (!hasMustChangePasswordColumn) {
            await this._exec(
                `
                    ALTER TABLE users
                    ADD COLUMN must_change_password TINYINT(1) NOT NULL DEFAULT 0
                    AFTER session_version;
                `
            );
        }

        const auditTableSpecs = [
            {
                table: 'stock_movement',
                dateColumn: 'stock_movement_date',
                userIdColumn: 'created_by_user_id',
                usernameColumn: 'created_by_username',
                indexName: 'idx_stock_movement_created_by_user_id',
                fkName: 'fk_stock_movement_created_by_user'
            },
            {
                table: 'finished_products',
                dateColumn: 'fin_product_date',
                userIdColumn: 'created_by_user_id',
                usernameColumn: 'created_by_username',
                indexName: 'idx_finished_products_created_by_user_id',
                fkName: 'fk_finished_products_created_by_user'
            }
        ];

        for (const spec of auditTableSpecs) {
            const hasUserIdColumn = await this._tableHasColumn(spec.table, spec.userIdColumn);
            if (!hasUserIdColumn) {
                await this._exec(
                    `
                        ALTER TABLE ${mysql.escapeId(spec.table)}
                        ADD COLUMN ${mysql.escapeId(spec.userIdColumn)} INT NULL
                        AFTER ${mysql.escapeId(spec.dateColumn)};
                    `
                );
            }

            const hasUsernameColumn = await this._tableHasColumn(spec.table, spec.usernameColumn);
            if (!hasUsernameColumn) {
                await this._exec(
                    `
                        ALTER TABLE ${mysql.escapeId(spec.table)}
                        ADD COLUMN ${mysql.escapeId(spec.usernameColumn)} VARCHAR(60) NULL
                        AFTER ${mysql.escapeId(spec.userIdColumn)};
                    `
                );
            }

            const hasIndex = await this._tableHasIndex(spec.table, spec.indexName);
            if (!hasIndex) {
                await this._exec(
                    `
                        CREATE INDEX ${mysql.escapeId(spec.indexName)}
                        ON ${mysql.escapeId(spec.table)} (${mysql.escapeId(spec.userIdColumn)});
                    `
                );
            }

            const hasConstraint = await this._tableHasConstraint(spec.table, spec.fkName);
            if (!hasConstraint) {
                await this._exec(
                    `
                        ALTER TABLE ${mysql.escapeId(spec.table)}
                        ADD CONSTRAINT ${mysql.escapeId(spec.fkName)}
                        FOREIGN KEY (${mysql.escapeId(spec.userIdColumn)})
                        REFERENCES users (user_id)
                        ON DELETE SET NULL;
                    `
                );
            }
        }

        this._authSchemaEnsured = true;
    }

    async ensureAppLogSchema() {
        if (this._appLogSchemaEnsured) return;

        await this._exec(
            `
                CREATE TABLE IF NOT EXISTS app_logs (
                    app_log_id INT NOT NULL AUTO_INCREMENT,
                    level VARCHAR(16) NOT NULL,
                    category VARCHAR(40) NOT NULL,
                    message VARCHAR(255) NOT NULL,
                    details_json LONGTEXT NULL,
                    actor_user_id INT NULL,
                    actor_username VARCHAR(60) NULL,
                    request_method VARCHAR(10) NULL,
                    request_path VARCHAR(255) NULL,
                    ip_address VARCHAR(120) NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (app_log_id),
                    KEY idx_app_logs_created_at (created_at),
                    KEY idx_app_logs_level (level),
                    KEY idx_app_logs_category (category),
                    KEY idx_app_logs_actor_user_id (actor_user_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
            `
        );

        const hasActorConstraint = await this._tableHasConstraint('app_logs', 'fk_app_logs_actor_user');
        if (!hasActorConstraint) {
            await this._exec(
                `
                    ALTER TABLE app_logs
                    ADD CONSTRAINT fk_app_logs_actor_user
                    FOREIGN KEY (actor_user_id)
                    REFERENCES users (user_id)
                    ON DELETE SET NULL;
                `
            );
        }

        this._appLogSchemaEnsured = true;
    }

    async hasAnyUsers() {
        try {
            const rows = await this._exec(
                `
                    SELECT COUNT(*) AS total
                    FROM users;
                `
            );

            return Number(rows?.[0]?.total || 0) > 0;
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    async createAppLog({
        level,
        category,
        message,
        details_json = null,
        actor_user_id = null,
        actor_username = null,
        request_method = null,
        request_path = null,
        ip_address = null
    }) {
        try {
            const result = await this._exec(
                `
                    INSERT INTO app_logs (
                        level,
                        category,
                        message,
                        details_json,
                        actor_user_id,
                        actor_username,
                        request_method,
                        request_path,
                        ip_address
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                `,
                [
                    level,
                    category,
                    message,
                    details_json,
                    actor_user_id,
                    actor_username,
                    request_method,
                    request_path,
                    ip_address
                ]
            );

            return {
                app_log_id: Number(result?.insertId || 0)
            };
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    async listAppLogs(filters = {}) {
        const {
            level = '',
            query = '',
            date_from = '',
            date_to = '',
            limit = 500
        } = filters;

        try {
            const whereClauses = [];
            const params = [];

            if (level) {
                whereClauses.push('level = ?');
                params.push(level);
            }

            if (query) {
                const likeValue = `%${query}%`;
                whereClauses.push(
                    `(
                        message LIKE ?
                        OR category LIKE ?
                        OR IFNULL(actor_username, '') LIKE ?
                        OR IFNULL(request_path, '') LIKE ?
                        OR IFNULL(details_json, '') LIKE ?
                    )`
                );
                params.push(likeValue, likeValue, likeValue, likeValue, likeValue);
            }

            if (date_from) {
                whereClauses.push('DATE(created_at) >= ?');
                params.push(date_from);
            }

            if (date_to) {
                whereClauses.push('DATE(created_at) <= ?');
                params.push(date_to);
            }

            const numericLimit = Math.max(1, Math.min(1000, Number(limit) || 500));
            const whereSql = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';

            const rows = await this._exec(
                `
                    SELECT
                        app_log_id,
                        level,
                        category,
                        message,
                        details_json,
                        actor_user_id,
                        actor_username,
                        request_method,
                        request_path,
                        ip_address,
                        created_at
                    FROM app_logs
                    ${whereSql}
                    ORDER BY app_log_id DESC
                    LIMIT ?;
                `,
                [...params, numericLimit]
            );

            return Array.isArray(rows)
                ? rows.map((row) => ({
                    app_log_id: Number(row.app_log_id),
                    level: String(row.level || ''),
                    category: String(row.category || ''),
                    message: String(row.message || ''),
                    details_json: row.details_json == null ? '' : String(row.details_json),
                    actor_user_id: row.actor_user_id == null ? null : Number(row.actor_user_id),
                    actor_username: row.actor_username == null ? '' : String(row.actor_username),
                    request_method: row.request_method == null ? '' : String(row.request_method),
                    request_path: row.request_path == null ? '' : String(row.request_path),
                    ip_address: row.ip_address == null ? '' : String(row.ip_address),
                    created_at: row.created_at
                }))
                : [];
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Creates production snapshot tables so finished-product assembly keeps a
    // frozen copy of the BOM used at the time of production.
    async ensureProductionSnapshotSchema() {
        if (this._productionSnapshotSchemaEnsured) {
            return { backfilled_runs: 0 };
        }

        await this._exec(
            `
                CREATE TABLE IF NOT EXISTS production_runs (
                    production_run_id INT NOT NULL AUTO_INCREMENT,
                    fin_product_id INT NOT NULL,
                    product_id INT NULL,
                    product_name_snapshot VARCHAR(255) NOT NULL,
                    produced_quantity DECIMAL(18,4) NOT NULL,
                    production_date DATETIME NOT NULL,
                    created_by_user_id INT NULL,
                    created_by_username VARCHAR(60) NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (production_run_id),
                    UNIQUE KEY uq_production_runs_fin_product_id (fin_product_id),
                    KEY idx_production_runs_product_id (product_id),
                    KEY idx_production_runs_production_date (production_date),
                    KEY idx_production_runs_created_by_user_id (created_by_user_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
            `
        );

        await this._exec(
            `
                CREATE TABLE IF NOT EXISTS production_run_materials (
                    production_run_material_id INT NOT NULL AUTO_INCREMENT,
                    production_run_id INT NOT NULL,
                    part_id INT NULL,
                    part_name_snapshot VARCHAR(255) NOT NULL,
                    tricoma_nr_snapshot VARCHAR(120) NULL,
                    quantity_per_product_snapshot DECIMAL(18,4) NOT NULL,
                    quantity_used DECIMAL(18,4) NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (production_run_material_id),
                    KEY idx_production_run_materials_run_id (production_run_id),
                    KEY idx_production_run_materials_part_id (part_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
            `
        );

        const constraintSpecs = [
            {
                table: 'production_runs',
                constraintName: 'fk_production_runs_finished_product',
                ddl: `
                    ALTER TABLE production_runs
                    ADD CONSTRAINT fk_production_runs_finished_product
                    FOREIGN KEY (fin_product_id)
                    REFERENCES finished_products (fin_product_id)
                    ON DELETE CASCADE;
                `
            },
            {
                table: 'production_runs',
                constraintName: 'fk_production_runs_created_by_user',
                ddl: `
                    ALTER TABLE production_runs
                    ADD CONSTRAINT fk_production_runs_created_by_user
                    FOREIGN KEY (created_by_user_id)
                    REFERENCES users (user_id)
                    ON DELETE SET NULL;
                `
            },
            {
                table: 'production_run_materials',
                constraintName: 'fk_production_run_materials_run',
                ddl: `
                    ALTER TABLE production_run_materials
                    ADD CONSTRAINT fk_production_run_materials_run
                    FOREIGN KEY (production_run_id)
                    REFERENCES production_runs (production_run_id)
                    ON DELETE CASCADE;
                `
            }
        ];

        for (const spec of constraintSpecs) {
            const hasConstraint = await this._tableHasConstraint(spec.table, spec.constraintName);
            if (!hasConstraint) {
                await this._exec(spec.ddl);
            }
        }

        const backfilledRuns = await this.backfillProductionRunSnapshots();
        this._productionSnapshotSchemaEnsured = true;
        return { backfilled_runs: backfilledRuns };
    }

    async _createProductionRunSnapshotOnConnection(connection, options) {
        const {
            fin_product_id,
            product_id,
            quantity,
            production_date,
            created_by_user_id = null,
            created_by_username = null
        } = options;

        const exec = (query, params = []) => this._execOnConnection(connection, query, params);
        const existingRows = await exec(
            `
                SELECT production_run_id
                FROM production_runs
                WHERE fin_product_id = ?
                LIMIT 1;
            `,
            [fin_product_id]
        );

        if (existingRows.length > 0) {
            return {
                production_run_id: Number(existingRows[0].production_run_id),
                materials_count: 0,
                created: false
            };
        }

        const quantityColumn = await this._getBomQuantityColumn();
        const partColumnSet = await this._getPartColumnSet();
        const tricomaColumn = this._getTricomaColumnFromPartColumnSet(partColumnSet);

        const productRows = await exec(
            `
                SELECT product_id, product_name
                FROM products
                WHERE product_id = ?
                LIMIT 1;
            `,
            [product_id]
        );

        const productNameSnapshot = String(productRows?.[0]?.product_name || `Product ${product_id}`);
        const producedQuantity = Number(quantity);
        const runInsertResult = await exec(
            `
                INSERT INTO production_runs (
                    fin_product_id,
                    product_id,
                    product_name_snapshot,
                    produced_quantity,
                    production_date,
                    created_by_user_id,
                    created_by_username
                )
                VALUES (?, ?, ?, ?, ?, ?, ?);
            `,
            [
                fin_product_id,
                product_id,
                productNameSnapshot,
                Number.isFinite(producedQuantity) ? producedQuantity : 0,
                production_date,
                created_by_user_id,
                created_by_username
            ]
        );

        const productionRunId = Number(runInsertResult.insertId);
        const bomRows = await exec(
            `
                SELECT
                    b.part_id,
                    COALESCE(p.part_name, CONCAT('Part ', b.part_id)) AS part_name_snapshot,
                    p.${mysql.escapeId(tricomaColumn)} AS tricoma_nr_snapshot,
                    b.${mysql.escapeId(quantityColumn)} AS quantity_per_product_snapshot
                FROM bom b
                LEFT JOIN parts p ON b.part_id = p.part_id
                WHERE b.product_id = ?
                ORDER BY b.part_id;
            `,
            [product_id]
        );

        let materialsCount = 0;
        for (const row of bomRows) {
            const quantityPerProduct = Number(row?.quantity_per_product_snapshot ?? 0);
            const quantityUsed = quantityPerProduct * (Number.isFinite(producedQuantity) ? producedQuantity : 0);

            await exec(
                `
                    INSERT INTO production_run_materials (
                        production_run_id,
                        part_id,
                        part_name_snapshot,
                        tricoma_nr_snapshot,
                        quantity_per_product_snapshot,
                        quantity_used
                    )
                    VALUES (?, ?, ?, ?, ?, ?);
                `,
                [
                    productionRunId,
                    Number(row?.part_id) || null,
                    String(row?.part_name_snapshot || `Part ${row?.part_id || ''}`),
                    row?.tricoma_nr_snapshot == null ? null : String(row.tricoma_nr_snapshot),
                    Number.isFinite(quantityPerProduct) ? quantityPerProduct : 0,
                    Number.isFinite(quantityUsed) ? quantityUsed : 0
                ]
            );
            materialsCount += 1;
        }

        return {
            production_run_id: productionRunId,
            materials_count: materialsCount,
            created: true
        };
    }

    async backfillProductionRunSnapshots() {
        return this._withTransaction(async (connection) => {
            const exec = (query, params = []) => this._execOnConnection(connection, query, params);
            const pendingRows = await exec(
                `
                    SELECT
                        fp.fin_product_id,
                        fp.product_id,
                        fp.fin_product_qty,
                        fp.fin_product_date,
                        fp.created_by_user_id,
                        fp.created_by_username
                    FROM finished_products fp
                    LEFT JOIN production_runs pr ON pr.fin_product_id = fp.fin_product_id
                    WHERE pr.production_run_id IS NULL
                    ORDER BY fp.fin_product_id;
                `
            );

            let backfilledRuns = 0;
            for (const row of pendingRows) {
                const snapshot = await this._createProductionRunSnapshotOnConnection(connection, {
                    fin_product_id: Number(row.fin_product_id),
                    product_id: Number(row.product_id),
                    quantity: Number(row.fin_product_qty),
                    production_date: row.fin_product_date,
                    created_by_user_id: row.created_by_user_id == null ? null : Number(row.created_by_user_id),
                    created_by_username: row.created_by_username == null ? null : String(row.created_by_username)
                });

                if (snapshot?.created) {
                    backfilledRuns += 1;
                }
            }

            return backfilledRuns;
        });
    }

    // Creates the planning/scheduling table used to queue future production work
    // without consuming stock immediately. Allocation is computed virtually from
    // live stock in due-date order.
    async ensureProductionScheduleSchema() {
        if (this._productionScheduleSchemaEnsured) {
            return;
        }

        await this._exec(
            `
                CREATE TABLE IF NOT EXISTS production_schedule_orders (
                    production_schedule_id INT NOT NULL AUTO_INCREMENT,
                    product_id INT NOT NULL,
                    planned_quantity DECIMAL(18,4) NOT NULL,
                    due_date DATE NOT NULL,
                    comments VARCHAR(500) NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    completed_at DATETIME NULL,
                    completed_by_user_id INT NULL,
                    completed_by_username VARCHAR(60) NULL,
                    fin_product_id INT NULL,
                    created_by_user_id INT NULL,
                    created_by_username VARCHAR(60) NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (production_schedule_id),
                    KEY idx_production_schedule_due_date (due_date),
                    KEY idx_production_schedule_product_id (product_id),
                    KEY idx_production_schedule_created_by_user_id (created_by_user_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
            `
        );

        const columnSpecs = [
            {
                columnName: 'comments',
                ddl: `
                    ALTER TABLE production_schedule_orders
                    ADD COLUMN comments VARCHAR(500) NULL AFTER due_date;
                `
            },
            {
                columnName: 'status',
                ddl: `
                    ALTER TABLE production_schedule_orders
                    ADD COLUMN status VARCHAR(20) NOT NULL DEFAULT 'pending' AFTER comments;
                `
            },
            {
                columnName: 'completed_at',
                ddl: `
                    ALTER TABLE production_schedule_orders
                    ADD COLUMN completed_at DATETIME NULL AFTER status;
                `
            },
            {
                columnName: 'completed_by_user_id',
                ddl: `
                    ALTER TABLE production_schedule_orders
                    ADD COLUMN completed_by_user_id INT NULL AFTER completed_at;
                `
            },
            {
                columnName: 'completed_by_username',
                ddl: `
                    ALTER TABLE production_schedule_orders
                    ADD COLUMN completed_by_username VARCHAR(60) NULL AFTER completed_by_user_id;
                `
            },
            {
                columnName: 'fin_product_id',
                ddl: `
                    ALTER TABLE production_schedule_orders
                    ADD COLUMN fin_product_id INT NULL AFTER completed_by_username;
                `
            }
        ];

        for (const spec of columnSpecs) {
            const hasColumn = await this._tableHasColumn('production_schedule_orders', spec.columnName);
            if (!hasColumn) {
                await this._exec(spec.ddl);
            }
        }

        const indexSpecs = [
            {
                indexName: 'idx_production_schedule_status',
                ddl: `
                    CREATE INDEX idx_production_schedule_status
                    ON production_schedule_orders (status);
                `
            },
            {
                indexName: 'idx_production_schedule_completed_by_user_id',
                ddl: `
                    CREATE INDEX idx_production_schedule_completed_by_user_id
                    ON production_schedule_orders (completed_by_user_id);
                `
            },
            {
                indexName: 'idx_production_schedule_fin_product_id',
                ddl: `
                    CREATE INDEX idx_production_schedule_fin_product_id
                    ON production_schedule_orders (fin_product_id);
                `
            }
        ];

        for (const spec of indexSpecs) {
            const hasIndex = await this._tableHasIndex('production_schedule_orders', spec.indexName);
            if (!hasIndex) {
                await this._exec(spec.ddl);
            }
        }

        const constraintSpecs = [
            {
                table: 'production_schedule_orders',
                constraintName: 'fk_production_schedule_product',
                ddl: `
                    ALTER TABLE production_schedule_orders
                    ADD CONSTRAINT fk_production_schedule_product
                    FOREIGN KEY (product_id)
                    REFERENCES products (product_id)
                    ON DELETE CASCADE;
                `
            },
            {
                table: 'production_schedule_orders',
                constraintName: 'fk_production_schedule_created_by_user',
                ddl: `
                    ALTER TABLE production_schedule_orders
                    ADD CONSTRAINT fk_production_schedule_created_by_user
                    FOREIGN KEY (created_by_user_id)
                    REFERENCES users (user_id)
                    ON DELETE SET NULL;
                `
            },
            {
                table: 'production_schedule_orders',
                constraintName: 'fk_production_schedule_completed_by_user',
                ddl: `
                    ALTER TABLE production_schedule_orders
                    ADD CONSTRAINT fk_production_schedule_completed_by_user
                    FOREIGN KEY (completed_by_user_id)
                    REFERENCES users (user_id)
                    ON DELETE SET NULL;
                `
            },
            {
                table: 'production_schedule_orders',
                constraintName: 'fk_production_schedule_finished_product',
                ddl: `
                    ALTER TABLE production_schedule_orders
                    ADD CONSTRAINT fk_production_schedule_finished_product
                    FOREIGN KEY (fin_product_id)
                    REFERENCES finished_products (fin_product_id)
                    ON DELETE SET NULL;
                `
            }
        ];

        for (const spec of constraintSpecs) {
            const hasConstraint = await this._tableHasConstraint(spec.table, spec.constraintName);
            if (!hasConstraint) {
                await this._exec(spec.ddl);
            }
        }

        this._productionScheduleSchemaEnsured = true;
    }

    // Seeds the first admin and Powermoon users only when no user rows exist yet.
    async seedUsersIfEmpty(users = []) {
        return this._withTransaction(async (connection) => {
            const exec = (query, params = []) => this._execOnConnection(connection, query, params);
            const countRows = await exec('SELECT COUNT(*) AS total FROM users;');
            const existingCount = Number(countRows?.[0]?.total || 0);

            if (existingCount > 0) {
                return { created: false, count: existingCount };
            }

            if (!Array.isArray(users) || users.length === 0) {
                throw new Error('No initial users are configured. Set DEFAULT_ADMIN_PASSWORD and DEFAULT_POWERMOON_PASSWORD before first startup.');
            }

            for (const user of users) {
                await exec(
                    `
                        INSERT INTO users (username, display_name, role, password_hash, must_change_password)
                        VALUES (?, ?, ?, ?, ?);
                    `,
                    [
                        user.username,
                        user.display_name,
                        user.role,
                        user.password_hash,
                        user.must_change_password ? 1 : 0
                    ]
                );
            }

            return { created: true, count: users.length };
        });
    }

    // ------------------------------------------------------------------------
    // Section 3: Authentication and User Management
    // ------------------------------------------------------------------------
    async getUserByUsername(username) {
        try {
            const rows = await this._exec(
                `
                    SELECT
                        user_id,
                        username,
                        display_name,
                        role,
                        password_hash,
                        session_version,
                        must_change_password,
                        created_at,
                        updated_at
                    FROM users
                    WHERE username = ?
                    LIMIT 1;
                `,
                [username]
            );

            return rows?.[0] || null;
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    async getUserById(user_id) {
        try {
            const rows = await this._exec(
                `
                    SELECT
                        user_id,
                        username,
                        display_name,
                        role,
                        password_hash,
                        session_version,
                        must_change_password,
                        created_at,
                        updated_at
                    FROM users
                    WHERE user_id = ?
                    LIMIT 1;
                `,
                [user_id]
            );

            return rows?.[0] || null;
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    async listUsers() {
        try {
            return await this._exec(
                `
                    SELECT
                        user_id,
                        username,
                        display_name,
                        role,
                        must_change_password,
                        created_at,
                        updated_at
                    FROM users
                    ORDER BY
                        CASE WHEN role = 'admin' THEN 0 ELSE 1 END,
                        username;
                `
            );
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    async countUsersByRole(role) {
        try {
            const rows = await this._exec(
                `
                    SELECT COUNT(*) AS total
                    FROM users
                    WHERE role = ?;
                `,
                [role]
            );

            return Number(rows?.[0]?.total || 0);
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    async createUser({ username, display_name, role, password_hash, must_change_password = false }) {
        try {
            const result = await this._exec(
                `
                    INSERT INTO users (username, display_name, role, password_hash, must_change_password)
                    VALUES (?, ?, ?, ?, ?);
                `,
                [username, display_name, role, password_hash, must_change_password ? 1 : 0]
            );

            return {
                user_id: result.insertId,
                username,
                display_name,
                role,
                must_change_password: Boolean(must_change_password)
            };
        } catch (error) {
            if (error?.message?.includes('Duplicate entry')) {
                throw new Error('Username already exists.');
            }

            console.log(error);
            throw error;
        }
    }

    async updateUser(user_id, { username, display_name, role, password_hash = null, must_change_password = null }) {
        return this._withTransaction(async (connection) => {
            const exec = (query, params = []) => this._execOnConnection(connection, query, params);
            const existingRows = await exec(
                `
                    SELECT user_id
                    FROM users
                    WHERE user_id = ?
                    LIMIT 1;
                `,
                [user_id]
            );

            if (!existingRows?.[0]) {
                return { user_found: false, user: null };
            }

            const setClauses = [
                'username = ?',
                'display_name = ?',
                'role = ?'
            ];
            const params = [username, display_name, role];

            if (password_hash) {
                setClauses.push('password_hash = ?');
                setClauses.push('session_version = session_version + 1');
                params.push(password_hash);
            }

            if (must_change_password !== null) {
                setClauses.push('must_change_password = ?');
                params.push(must_change_password ? 1 : 0);
            }

            params.push(user_id);

            try {
                await exec(
                    `
                        UPDATE users
                        SET ${setClauses.join(', ')}
                        WHERE user_id = ?;
                    `,
                    params
                );
            } catch (error) {
                if (error?.message?.includes('Duplicate entry')) {
                    throw new Error('Username already exists.');
                }

                throw error;
            }

            const updatedRows = await exec(
                `
                    SELECT
                        user_id,
                        username,
                        display_name,
                        role,
                        password_hash,
                        session_version,
                        must_change_password,
                        created_at,
                        updated_at
                    FROM users
                    WHERE user_id = ?
                    LIMIT 1;
                `,
                [user_id]
            );

            return {
                user_found: true,
                user: updatedRows?.[0] || null
            };
        });
    }

    async updateUserPassword(user_id, password_hash) {
        return this._withTransaction(async (connection) => {
            const exec = (query, params = []) => this._execOnConnection(connection, query, params);
            const existingRows = await exec(
                `
                    SELECT user_id
                    FROM users
                    WHERE user_id = ?
                    LIMIT 1;
                `,
                [user_id]
            );

            if (!existingRows?.[0]) {
                return { user_found: false, user: null };
            }

            await exec(
                `
                    UPDATE users
                    SET password_hash = ?, session_version = session_version + 1, must_change_password = 0
                    WHERE user_id = ?;
                `,
                [password_hash, user_id]
            );

            const updatedRows = await exec(
                `
                    SELECT
                        user_id,
                        username,
                        display_name,
                        role,
                        password_hash,
                        session_version,
                        must_change_password,
                        created_at,
                        updated_at
                    FROM users
                    WHERE user_id = ?
                    LIMIT 1;
                `,
                [user_id]
            );

            return {
                user_found: true,
                user: updatedRows?.[0] || null
            };
        });
    }

    async deleteUser(user_id) {
        try {
            return await this._exec(
                `
                    DELETE FROM users
                    WHERE user_id = ?;
                `,
                [user_id]
            );
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // ------------------------------------------------------------------------
    // Section 4: Part Records and BOM Relations
    // ------------------------------------------------------------------------
    // Returns lightweight part rows for dropdowns and selectors.
    async getAllData() {
        try {
            return await this._exec('SELECT part_id, part_name FROM parts;');
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Returns full part catalog rows for CSV export and bulk editing.
    async getPartsCatalogForCsv() {
        try {
            const partColumnSet = await this._getPartColumnSet();
            const tricomaColumn = this._getTricomaColumnFromPartColumnSet(partColumnSet);
            const reorderLevelExpression = partColumnSet.has('reorder_level')
                ? 'COALESCE(reorder_level, 0)'
                : '0';
            const reorderQuantityExpression = partColumnSet.has('reorder_quantity')
                ? 'COALESCE(reorder_quantity, 0)'
                : '0';

            return await this._exec(
                `
                    SELECT
                        part_id,
                        part_name,
                        ${mysql.escapeId(tricomaColumn)} AS tricoma_nr,
                        ${reorderLevelExpression} AS reorder_level,
                        ${reorderQuantityExpression} AS reorder_quantity
                    FROM parts
                    ORDER BY part_id;
                `
            );
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Creates or updates standalone part catalog rows without requiring BOM usages.
    async importPartsCatalogRows(rows = []) {
        return this._withTransaction(async (connection) => {
            const exec = (query, params = []) => this._execOnConnection(connection, query, params);
            const safeRows = Array.isArray(rows) ? rows : [];
            const partColumnSet = await this._getPartColumnSet();
            const tricomaColumn = this._getTricomaColumnFromPartColumnSet(partColumnSet);
            const canStoreReorderLevel = partColumnSet.has('reorder_level');
            const canStoreReorderQuantity = partColumnSet.has('reorder_quantity');

            let createdCount = 0;
            let updatedCount = 0;

            for (const row of safeRows) {
                const partId = row?.part_id == null ? null : Number(row.part_id);
                const normalizedPartName = String(row.part_name);
                const normalizedTricoma = String(row.tricoma_nr);
                const reorderLevel = Number(row.reorder_level);
                const reorderQuantity = Number(row.reorder_quantity);

                let existingPart = null;

                if (Number.isInteger(partId) && partId > 0) {
                    const existingById = await exec(
                        `
                            SELECT part_id
                            FROM parts
                            WHERE part_id = ?
                            LIMIT 1
                            FOR UPDATE;
                        `,
                        [partId]
                    );

                    if (existingById.length === 0) {
                        throw new Error(`Part id ${partId} was not found while importing parts CSV.`);
                    }

                    existingPart = { part_id: partId };
                } else {
                    const existingByIdentity = await exec(
                        `
                            SELECT part_id
                            FROM parts
                            WHERE ${mysql.escapeId(tricomaColumn)} = ?
                               OR part_name = ?
                            ORDER BY part_id
                            LIMIT 1
                            FOR UPDATE;
                        `,
                        [normalizedTricoma, normalizedPartName]
                    );

                    existingPart = existingByIdentity[0] || null;
                }

                if (existingPart) {
                    const updateClauses = [
                        'part_name = ?',
                        `${mysql.escapeId(tricomaColumn)} = ?`
                    ];
                    const updateParams = [
                        normalizedPartName,
                        normalizedTricoma
                    ];

                    if (canStoreReorderLevel) {
                        updateClauses.push('reorder_level = ?');
                        updateParams.push(reorderLevel);
                    }

                    if (canStoreReorderQuantity) {
                        updateClauses.push('reorder_quantity = ?');
                        updateParams.push(reorderQuantity);
                    }

                    updateParams.push(Number(existingPart.part_id));

                    await exec(
                        `
                            UPDATE parts
                            SET ${updateClauses.join(', ')}
                            WHERE part_id = ?;
                        `,
                        updateParams
                    );
                    updatedCount += 1;
                    continue;
                }

                const insertColumns = ['part_name', tricomaColumn];
                const insertValues = [normalizedPartName, normalizedTricoma];

                if (canStoreReorderLevel) {
                    insertColumns.push('reorder_level');
                    insertValues.push(reorderLevel);
                }

                if (canStoreReorderQuantity) {
                    insertColumns.push('reorder_quantity');
                    insertValues.push(reorderQuantity);
                }

                if (partColumnSet.has('tracked_stock')) {
                    insertColumns.push('tracked_stock');
                    insertValues.push(0);
                }

                await exec(
                    `
                        INSERT INTO parts (${insertColumns.map((column) => mysql.escapeId(column)).join(', ')})
                        VALUES (${insertColumns.map(() => '?').join(', ')});
                    `,
                    insertValues
                );
                createdCount += 1;
            }

            return {
                row_count: safeRows.length,
                created_count: createdCount,
                updated_count: updatedCount
            };
        });
    }

    // Creates one part and its initial product-usage relations atomically.
    // Supports small schema differences by discovering relation table/columns.
    async insertPartWithUsages({ part_name, tricoma_nr, reorder_level, reorder_quantity, usages = [] }) {
        return this._withTransaction(async (connection) => {
            const exec = (query, params = []) => this._execOnConnection(connection, query, params);

            // Inspect the schema so the code can work across small naming differences.
            const partColumnSet = await this._getPartColumnSet();

            if (!partColumnSet.has('part_name')) {
                throw new Error('The parts table is missing required column part_name.');
            }

            const tricomaColumn = this._getTricomaColumnFromPartColumnSet(partColumnSet);

            const insertColumns = ['part_name', tricomaColumn];
            const insertValues = [part_name, tricoma_nr];

            if (partColumnSet.has('reorder_level')) {
                insertColumns.push('reorder_level');
                insertValues.push(reorder_level);
            }

            if (partColumnSet.has('reorder_quantity')) {
                insertColumns.push('reorder_quantity');
                insertValues.push(reorder_quantity);
            }

            if (partColumnSet.has('tracked_stock')) {
                insertColumns.push('tracked_stock');
                insertValues.push(0);
            }

            const insertPartSql = `
                INSERT INTO parts (${insertColumns.map((name) => mysql.escapeId(name)).join(', ')})
                VALUES (${insertColumns.map(() => '?').join(', ')});
            `;

            const partInsertResult = await exec(insertPartSql, insertValues);
            const partId = partInsertResult.insertId;

            if (usages.length > 0) {
                const tableToColumns = new Map();

                // Find a relation table dynamically instead of hard-coding one name.
                const tablesResult = await exec('SHOW TABLES;');
                const tableNameKey = Object.keys(tablesResult[0] || {})[0];
                const tableNames = tablesResult.map((row) => row[tableNameKey]);

                for (const tableName of tableNames) {
                    const columns = await exec(`SHOW COLUMNS FROM ${mysql.escapeId(tableName)};`);
                    tableToColumns.set(
                        tableName,
                        new Set(columns.map(({ Field }) => String(Field).toLowerCase()))
                    );
                }

                const relationTableCandidates = [];
                tableToColumns.forEach((columns, tableName) => {
                    if (columns.has('part_id') && columns.has('product_id')) {
                        relationTableCandidates.push({ tableName, columns });
                    }
                });

                if (relationTableCandidates.length === 0) {
                    throw new Error('Could not find a part-to-product relation table (needs part_id and product_id columns).');
                }

                const preferredPatterns = [/bom/i, /recipe/i, /usage/i, /component/i, /part_product/i, /product_part/i, /assembly/i];
                relationTableCandidates.sort((a, b) => {
                    const score = (name) => {
                        const matchedPatternIdx = preferredPatterns.findIndex((pattern) => pattern.test(name));
                        return matchedPatternIdx >= 0 ? matchedPatternIdx : 999;
                    };

                    const scoreA = score(a.tableName);
                    const scoreB = score(b.tableName);

                    if (scoreA !== scoreB) return scoreA - scoreB;
                    return a.tableName.localeCompare(b.tableName);
                });

                const selectedRelationTable = relationTableCandidates[0];

                const quantityCandidates = ['quantity_per_product', 'qty_per_product', 'part_qty', 'bom_qty', 'quantity', 'qty'];
                const quantityColumn = quantityCandidates.find((name) => selectedRelationTable.columns.has(name));

                if (!quantityColumn) {
                    throw new Error(`Relation table ${selectedRelationTable.tableName} has no quantity column for per-product part usage.`);
                }

                for (const usage of usages) {
                    const relationColumns = ['part_id', 'product_id', quantityColumn];
                    const relationValues = [partId, usage.product_id, usage.quantity_per_product];

                    const insertRelationSql = `
                        INSERT INTO ${mysql.escapeId(selectedRelationTable.tableName)}
                        (${relationColumns.map((name) => mysql.escapeId(name)).join(', ')})
                        VALUES (${relationColumns.map(() => '?').join(', ')});
                    `;

                    await exec(insertRelationSql, relationValues);
                }
            }

            return {
                part_id: partId,
                part_name,
                tricoma_nr,
                reorder_level,
                reorder_quantity,
                usages_count: usages.length
            };
        });
    }

    // Deletes one part and dependent rows that reference it.
    // Returns flags and affected row counts for API feedback.
    async deletePartWithRelations(part_id) {
        return this._withTransaction(async (connection) => {
            const exec = (query, params = []) => this._execOnConnection(connection, query, params);

            const stockMovementDeleteResult = await exec(
                `
                    DELETE FROM stock_movement
                    WHERE part_id = ?;
                `,
                [part_id]
            );

            const bomDeleteResult = await exec(
                `
                    DELETE FROM bom
                    WHERE part_id = ?;
                `,
                [part_id]
            );

            const partDeleteResult = await exec(
                `
                    DELETE FROM parts
                    WHERE part_id = ?;
                `,
                [part_id]
            );

            return {
                part_id,
                part_deleted: partDeleteResult.affectedRows > 0,
                stock_movement_deleted_rows: stockMovementDeleteResult.affectedRows,
                bom_deleted_rows: bomDeleteResult.affectedRows
            };
        });
    }

    // Resolves the BOM quantity column name across supported schema variants.
    async _getBomQuantityColumn() {
        if (!this._bomQuantityColumnCache) {
            const bomColumns = await this._exec('SHOW COLUMNS FROM bom;');
            const columnSet = new Set(bomColumns.map(({ Field }) => String(Field).toLowerCase()));
            // Older/newer schemas may use different column names for the same concept,
            // so this helper chooses the first compatible quantity field it can find.
            const quantityCandidates = ['bom_qty', 'quantity_per_product', 'qty_per_product', 'part_qty', 'quantity', 'qty'];
            const quantityColumn = quantityCandidates.find((name) => columnSet.has(name));

            if (!quantityColumn) {
                throw new Error('Could not find quantity column in bom table.');
            }

            this._bomQuantityColumnCache = quantityColumn;
        }

        return this._bomQuantityColumnCache;
    }

    // Resolves the Tricoma column name on `parts` across known variants.
    _getTricomaColumnFromPartColumnSet(partColumnSet) {
        if (this._tricomaColumnCache) {
            return this._tricomaColumnCache;
        }

        // Keep compatibility with installations that imported slightly different
        // schemas or renamed the Tricoma field over time.
        const tricomaCandidates = ['tricoma_nr', 'tricoma_no', 'tricoma_number'];
        const tricomaColumn = tricomaCandidates.find((name) => partColumnSet.has(name));

        if (!tricomaColumn) {
            throw new Error('Could not find a Tricoma column in parts table (expected tricoma_nr, tricoma_no, or tricoma_number).');
        }

        this._tricomaColumnCache = tricomaColumn;
        return this._tricomaColumnCache;
    }

    // Returns editable fields for one part detail form.
    async getPartDetails(part_id) {
        try {
            const partColumnSet = await this._getPartColumnSet();
            const tricomaColumn = this._getTricomaColumnFromPartColumnSet(partColumnSet);

            if (!partColumnSet.has('reorder_level') || !partColumnSet.has('reorder_quantity')) {
                throw new Error('The parts table is missing reorder_level and/or reorder_quantity columns.');
            }

            const rows = await this._exec(
                `
                    SELECT
                        part_id,
                        part_name,
                        ${mysql.escapeId(tricomaColumn)} AS tricoma_nr,
                        reorder_level,
                        reorder_quantity
                    FROM parts
                    WHERE part_id = ?
                    LIMIT 1;
                `,
                [part_id]
            );

            if (rows.length === 0) {
                return null;
            }

            return rows[0];
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Updates editable part fields in one transaction-safe flow.
    async updatePartDetails(part_id, { part_name, tricoma_nr, reorder_level, reorder_quantity }) {
        return this._withTransaction(async (connection) => {
            const exec = (query, params = []) => this._execOnConnection(connection, query, params);

            const partColumnSet = await this._getPartColumnSet();
            const tricomaColumn = this._getTricomaColumnFromPartColumnSet(partColumnSet);

            if (!partColumnSet.has('part_name') || !partColumnSet.has('reorder_level') || !partColumnSet.has('reorder_quantity')) {
                throw new Error('The parts table is missing part_name, reorder_level, and/or reorder_quantity columns.');
            }

            const partRows = await exec(
                `
                    SELECT part_id
                    FROM parts
                    WHERE part_id = ?
                    LIMIT 1
                    FOR UPDATE;
                `,
                [part_id]
            );

            if (partRows.length === 0) {
                return { part_id, part_found: false };
            }

            const updateResult = await exec(
                `
                    UPDATE parts
                    SET
                        part_name = ?,
                        ${mysql.escapeId(tricomaColumn)} = ?,
                        reorder_level = ?,
                        reorder_quantity = ?
                    WHERE part_id = ?;
                `,
                [part_name, tricoma_nr, reorder_level, reorder_quantity, part_id]
            );

            return {
                part_id,
                part_found: true,
                part_name,
                tricoma_nr,
                reorder_level,
                reorder_quantity,
                updated_rows: updateResult.affectedRows
            };
        });
    }

    // Returns current BOM relations for a selected part.
    async getPartBomRelations(part_id) {
        try {
            const partRows = await this._exec(
                `
                    SELECT part_id
                    FROM parts
                    WHERE part_id = ?
                    LIMIT 1;
                `,
                [part_id]
            );

            if (partRows.length === 0) {
                return null;
            }

            const quantityColumn = await this._getBomQuantityColumn();

            const usages = await this._exec(
                `
                    SELECT
                        b.product_id,
                        p.product_name,
                        b.${mysql.escapeId(quantityColumn)} AS quantity_per_product
                    FROM bom b
                    LEFT JOIN products p ON b.product_id = p.product_id
                    WHERE b.part_id = ?
                    ORDER BY p.product_name, b.product_id;
                `,
                [part_id]
            );

            return {
                part_id,
                usages
            };
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Replaces all BOM relations for one part in a single transaction.
    async updatePartBomRelations(part_id, usages = []) {
        return this._withTransaction(async (connection) => {
            const exec = (query, params = []) => this._execOnConnection(connection, query, params);

            const partRows = await exec(
                `
                    SELECT part_id
                    FROM parts
                    WHERE part_id = ?
                    LIMIT 1
                    FOR UPDATE;
                `,
                [part_id]
            );

            if (partRows.length === 0) {
                return { part_id, part_found: false };
            }

            const quantityColumn = await this._getBomQuantityColumn();

            const deleteResult = await exec(
                `
                    DELETE FROM bom
                    WHERE part_id = ?;
                `,
                [part_id]
            );

            let insertedRows = 0;
            for (const usage of usages) {
                const insertResult = await exec(
                    `
                        INSERT INTO bom (part_id, product_id, ${mysql.escapeId(quantityColumn)})
                        VALUES (?, ?, ?);
                    `,
                    [part_id, usage.product_id, usage.quantity_per_product]
                );
                insertedRows += insertResult.affectedRows;
            }

            return {
                part_id,
                part_found: true,
                removed_relations: deleteResult.affectedRows,
                inserted_relations: insertedRows
            };
        });
    }

    // ------------------------------------------------------------------------
    // Section 4: Products and Buildability
    // ------------------------------------------------------------------------
    // Returns product list for dropdowns and lightweight lookups.
    async getAllProducts() {
        try {
            return await this._exec('SELECT product_id, product_name FROM products;');
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Returns the product catalog for CSV export and bulk editing.
    async getProductsCatalogForCsv() {
        try {
            return await this._exec(
                `
                    SELECT product_id, product_name
                    FROM products
                    ORDER BY product_id;
                `
            );
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Creates or updates products from imported CSV rows.
    async importProductsCatalogRows(rows = []) {
        return this._withTransaction(async (connection) => {
            const exec = (query, params = []) => this._execOnConnection(connection, query, params);
            const safeRows = Array.isArray(rows) ? rows : [];
            let createdCount = 0;
            let updatedCount = 0;

            for (const row of safeRows) {
                const productId = row?.product_id == null ? null : Number(row.product_id);
                const productName = String(row.product_name);
                let existingProduct = null;

                if (Number.isInteger(productId) && productId > 0) {
                    const existingById = await exec(
                        `
                            SELECT product_id
                            FROM products
                            WHERE product_id = ?
                            LIMIT 1
                            FOR UPDATE;
                        `,
                        [productId]
                    );

                    if (existingById.length === 0) {
                        throw new Error(`Product id ${productId} was not found while importing products CSV.`);
                    }

                    existingProduct = { product_id: productId };
                } else {
                    const existingByName = await exec(
                        `
                            SELECT product_id
                            FROM products
                            WHERE product_name = ?
                            ORDER BY product_id
                            LIMIT 1
                            FOR UPDATE;
                        `,
                        [productName]
                    );

                    existingProduct = existingByName[0] || null;
                }

                if (existingProduct) {
                    await exec(
                        `
                            UPDATE products
                            SET product_name = ?
                            WHERE product_id = ?;
                        `,
                        [productName, Number(existingProduct.product_id)]
                    );
                    updatedCount += 1;
                    continue;
                }

                await exec(
                    `
                        INSERT INTO products (product_name)
                        VALUES (?);
                    `,
                    [productName]
                );
                createdCount += 1;
            }

            return {
                row_count: safeRows.length,
                created_count: createdCount,
                updated_count: updatedCount
            };
        });
    }

    // Creates one product row and returns created identifiers.
    async insertProduct(product_name) {
        try {
            const result = await this._exec(
                `
                    INSERT INTO products (product_name)
                    VALUES (?);
                `,
                [product_name]
            );

            return {
                product_id: result.insertId,
                product_name
            };
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Returns BOM rows with both ids and human-readable labels for CSV export.
    async getBomCatalogForCsv() {
        try {
            const quantityColumn = await this._getBomQuantityColumn();
            const partColumnSet = await this._getPartColumnSet();
            const tricomaColumn = this._getTricomaColumnFromPartColumnSet(partColumnSet);

            return await this._exec(
                `
                    SELECT
                        b.product_id,
                        prod.product_name,
                        b.part_id,
                        p.part_name,
                        p.${mysql.escapeId(tricomaColumn)} AS tricoma_nr,
                        b.${mysql.escapeId(quantityColumn)} AS quantity_per_product
                    FROM bom b
                    LEFT JOIN products prod ON b.product_id = prod.product_id
                    LEFT JOIN parts p ON b.part_id = p.part_id
                    ORDER BY prod.product_name, p.part_name, b.product_id, b.part_id;
                `
            );
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Creates or updates BOM rows from CSV so product-part relations can be managed in bulk.
    async importBomCatalogRows(rows = []) {
        return this._withTransaction(async (connection) => {
            const exec = (query, params = []) => this._execOnConnection(connection, query, params);
            const safeRows = Array.isArray(rows) ? rows : [];
            const quantityColumn = await this._getBomQuantityColumn();
            const partColumnSet = await this._getPartColumnSet();
            const tricomaColumn = this._getTricomaColumnFromPartColumnSet(partColumnSet);
            let createdCount = 0;
            let updatedCount = 0;

            const productRows = await exec(
                `
                    SELECT product_id, product_name
                    FROM products;
                `
            );
            const partRows = await exec(
                `
                    SELECT
                        part_id,
                        part_name,
                        ${mysql.escapeId(tricomaColumn)} AS tricoma_nr
                    FROM parts;
                `
            );

            const productIdSet = new Set(productRows.map((row) => Number(row.product_id)));
            const productNameMap = new Map(
                productRows.map((row) => [String(row.product_name).trim().toLowerCase(), Number(row.product_id)])
            );
            const partIdSet = new Set(partRows.map((row) => Number(row.part_id)));
            const partNameMap = new Map(
                partRows.map((row) => [String(row.part_name).trim().toLowerCase(), Number(row.part_id)])
            );
            const tricomaMap = new Map(
                partRows
                    .filter((row) => row.tricoma_nr != null && String(row.tricoma_nr).trim() !== '')
                    .map((row) => [String(row.tricoma_nr).trim(), Number(row.part_id)])
            );

            for (const row of safeRows) {
                const productId = row?.product_id == null ? null : Number(row.product_id);
                const partId = row?.part_id == null ? null : Number(row.part_id);
                const productNameKey = String(row.product_name ?? '').trim().toLowerCase();
                const partNameKey = String(row.part_name ?? '').trim().toLowerCase();
                const tricomaNr = String(row.tricoma_nr ?? '').trim();
                const quantityPerProduct = Number(row.quantity_per_product);

                let resolvedProductId = null;
                if (Number.isInteger(productId) && productId > 0) {
                    if (!productIdSet.has(productId)) {
                        throw new Error(`Product id ${productId} was not found while importing BOM CSV.`);
                    }
                    resolvedProductId = productId;
                } else if (productNameKey && productNameMap.has(productNameKey)) {
                    resolvedProductId = Number(productNameMap.get(productNameKey));
                } else {
                    throw new Error(`Unable to resolve product for BOM row "${row.product_name || row.product_id || ''}".`);
                }

                let resolvedPartId = null;
                if (Number.isInteger(partId) && partId > 0) {
                    if (!partIdSet.has(partId)) {
                        throw new Error(`Part id ${partId} was not found while importing BOM CSV.`);
                    }
                    resolvedPartId = partId;
                } else if (tricomaNr && tricomaMap.has(tricomaNr)) {
                    resolvedPartId = Number(tricomaMap.get(tricomaNr));
                } else if (partNameKey && partNameMap.has(partNameKey)) {
                    resolvedPartId = Number(partNameMap.get(partNameKey));
                } else {
                    throw new Error(`Unable to resolve part for BOM row "${row.part_name || row.tricoma_nr || row.part_id || ''}".`);
                }

                const existingRelation = await exec(
                    `
                        SELECT product_id, part_id
                        FROM bom
                        WHERE product_id = ? AND part_id = ?
                        LIMIT 1;
                    `,
                    [resolvedProductId, resolvedPartId]
                );

                if (existingRelation.length > 0) {
                    await exec(
                        `
                            UPDATE bom
                            SET ${mysql.escapeId(quantityColumn)} = ?
                            WHERE product_id = ? AND part_id = ?;
                        `,
                        [quantityPerProduct, resolvedProductId, resolvedPartId]
                    );
                    updatedCount += 1;
                    continue;
                }

                await exec(
                    `
                        INSERT INTO bom (product_id, part_id, ${mysql.escapeId(quantityColumn)})
                        VALUES (?, ?, ?);
                    `,
                    [resolvedProductId, resolvedPartId, quantityPerProduct]
                );
                createdCount += 1;
            }

            return {
                row_count: safeRows.length,
                created_count: createdCount,
                updated_count: updatedCount
            };
        });
    }

    // Deletes one product and all dependent rows that reference it.
    async deleteProductWithRelations(product_id) {
        return this._withTransaction(async (connection) => {
            const exec = (query, params = []) => this._execOnConnection(connection, query, params);

            const bomDeleteResult = await exec(
                `
                    DELETE FROM bom
                    WHERE product_id = ?;
                `,
                [product_id]
            );

            const finishedDeleteResult = await exec(
                `
                    DELETE FROM finished_products
                    WHERE product_id = ?;
                `,
                [product_id]
            );

            const productDeleteResult = await exec(
                `
                    DELETE FROM products
                    WHERE product_id = ?;
                `,
                [product_id]
            );

            return {
                product_id,
                product_deleted: productDeleteResult.affectedRows > 0,
                bom_deleted_rows: bomDeleteResult.affectedRows,
                finished_products_deleted_rows: finishedDeleteResult.affectedRows
            };
        });
    }

    // Computes requirements and max buildable units for one product.
    // Limiting parts are those tied for the minimum buildable count.
    async getProductBuildability(product_id) {
        try {
            const productRows = await this._exec(
                `
                    SELECT product_id, product_name
                    FROM products
                    WHERE product_id = ?
                    LIMIT 1;
                `,
                [product_id]
            );

            if (productRows.length === 0) {
                return null;
            }

            const quantityColumn = await this._getBomQuantityColumn();
            const partColumnSet = await this._getPartColumnSet();
            const tricomaColumn = this._getTricomaColumnFromPartColumnSet(partColumnSet);
            const partStockSql = await this._buildPartStockSnapshotSql();
            const reorderQuantityExpression = partColumnSet.has('reorder_quantity')
                ? 'COALESCE(v.reorder_quantity, p.reorder_quantity, 0)'
                : '0';

            // Join BOM requirements to the snapshot-aware stock aggregate so
            // buildability stays frozen even if the live BOM changes later.
            const requirements = await this._exec(
                `
                    SELECT
                        b.part_id,
                        p.part_name,
                        p.${mysql.escapeId(tricomaColumn)} AS tricoma_nr,
                        b.${mysql.escapeId(quantityColumn)} AS quantity_per_product,
                        ${reorderQuantityExpression} AS reorder_quantity,
                        COALESCE(v.tracked_stock, 0) AS tracked_stock
                    FROM bom b
                    LEFT JOIN parts p ON b.part_id = p.part_id
                    LEFT JOIN (${partStockSql}) v ON b.part_id = v.part_id
                    WHERE b.product_id = ?
                    ORDER BY p.part_name, b.part_id;
                `,
                [product_id]
            );

            let maxBuildable = 0;
            let limitingParts = [];

            if (requirements.length > 0) {
                // Convert each required part into the number of full products it can support.
                const candidateCounts = requirements.map((row) => {
                    const requiredQty = Number(row.quantity_per_product);
                    const trackedStock = Number(row.tracked_stock);
                    const partId = Number(row.part_id);
                    const safeRequiredQty = Number.isFinite(requiredQty) ? requiredQty : 0;
                    const safeTrackedStock = Number.isFinite(trackedStock) ? trackedStock : 0;

                    let buildableUnits = 0;
                    if (safeRequiredQty > 0 && safeTrackedStock > 0) {
                        buildableUnits = Math.floor(safeTrackedStock / safeRequiredQty);
                    }

                    return {
                        part_id: Number.isInteger(partId) && partId > 0 ? partId : null,
                        part_name: row.part_name || null,
                        tricoma_nr: row.tricoma_nr || null,
                        quantity_per_product: safeRequiredQty,
                        reorder_quantity: Number(row.reorder_quantity ?? 0),
                        tracked_stock: safeTrackedStock,
                        buildable_units: buildableUnits
                    };
                });

                // Limiting parts are all parts that share the minimum buildable unit count.
                maxBuildable = Math.min(...candidateCounts.map((row) => row.buildable_units));
                limitingParts = candidateCounts
                    .filter((row) => row.buildable_units === maxBuildable)
                    .map((row) => ({
                        part_id: row.part_id,
                        part_name: row.part_name,
                        tricoma_nr: row.tricoma_nr,
                        quantity_per_product: row.quantity_per_product,
                        reorder_quantity: row.reorder_quantity,
                        tracked_stock: row.tracked_stock,
                        buildable_units: row.buildable_units
                    }));
            }

            return {
                product_id,
                product_name: productRows[0].product_name,
                max_buildable: maxBuildable,
                requirements,
                limiting_parts: limitingParts
            };
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Computes the material plan for a requested finished-product quantity.
    async getProductionPlan(product_id, quantity) {
        try {
            const requestedQuantity = Number(quantity);
            const buildability = await this.getProductBuildability(product_id);

            if (!buildability) {
                return null;
            }

            // Expand each BOM requirement from "per product" into the actual
            // quantities needed for the user's requested production run.
            const materials = Array.isArray(buildability.requirements)
                ? buildability.requirements.map((row) => {
                    const quantityPerProduct = Number(row.quantity_per_product);
                    const trackedStock = Number(row.tracked_stock);
                    const requiredQuantity = quantityPerProduct * requestedQuantity;
                    const remainingAfterPlan = trackedStock - requiredQuantity;

                    return {
                        part_id: Number(row.part_id),
                        part_name: row.part_name || null,
                        tricoma_nr: row.tricoma_nr || null,
                        quantity_per_product: Number.isFinite(quantityPerProduct) ? quantityPerProduct : 0,
                        required_quantity: Number.isFinite(requiredQuantity) ? requiredQuantity : 0,
                        reorder_quantity: Number(row.reorder_quantity ?? 0),
                        tracked_stock: Number.isFinite(trackedStock) ? trackedStock : 0,
                        remaining_after_plan: Number.isFinite(remainingAfterPlan) ? remainingAfterPlan : 0,
                        shortage_quantity: remainingAfterPlan < 0 ? Math.abs(remainingAfterPlan) : 0,
                        has_enough_stock: remainingAfterPlan >= 0
                    };
                })
                : [];

            // Downstream PDF exports and UI warnings only care about parts that will
            // actually run short, so keep that subset ready alongside the full list.
            const missingMaterials = materials.filter((row) => !row.has_enough_stock);

            return {
                product_id: buildability.product_id,
                product_name: buildability.product_name,
                requested_quantity: requestedQuantity,
                max_buildable: buildability.max_buildable,
                can_fulfill: requestedQuantity <= Number(buildability.max_buildable ?? 0),
                materials,
                missing_materials: missingMaterials,
                limiting_parts: buildability.limiting_parts
            };
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    async _getPendingProductionScheduleRows() {
        return this._exec(
            `
                SELECT
                    pso.production_schedule_id,
                    pso.product_id,
                    prod.product_name,
                    pso.planned_quantity,
                    pso.due_date,
                    pso.comments,
                    pso.status,
                    pso.completed_at,
                    pso.completed_by_user_id,
                    COALESCE(completed_user.display_name, completed_user.username, pso.completed_by_username, '') AS completed_by,
                    pso.fin_product_id,
                    pso.created_by_user_id,
                    COALESCE(u.display_name, u.username, pso.created_by_username, 'Legacy') AS created_by,
                    pso.created_at
                FROM production_schedule_orders pso
                LEFT JOIN products prod ON pso.product_id = prod.product_id
                LEFT JOIN users u ON pso.created_by_user_id = u.user_id
                LEFT JOIN users completed_user ON pso.completed_by_user_id = completed_user.user_id
                ORDER BY
                    CASE
                        WHEN pso.status = 'pending' THEN 0
                        WHEN pso.status = 'completed' THEN 1
                        ELSE 2
                    END ASC,
                    CASE WHEN pso.status = 'pending' THEN pso.due_date END ASC,
                    CASE WHEN pso.status = 'completed' THEN pso.completed_at END DESC,
                    pso.created_at ASC,
                    pso.production_schedule_id ASC;
            `
        );
    }

    // Builds a virtual reservation plan for all pending production orders. The
    // earliest due production claims stock first, and later productions only see
    // what remains after those earlier reservations.
    async getProductionScheduleOverview() {
        try {
            const orderRows = await this._getPendingProductionScheduleRows();

            if (!Array.isArray(orderRows) || orderRows.length === 0) {
                return {
                    orders: [],
                    meta: {
                        pending_orders: 0,
                        scheduled_units: 0,
                        allocated_orders: 0,
                        shortage_orders: 0,
                        next_due_date: ''
                    }
                };
            }

            const pendingRows = orderRows.filter((row) => String(row?.status || 'pending').trim().toLowerCase() === 'pending');
            const productIds = [...new Set(
                pendingRows
                    .map((row) => Number(row?.product_id))
                    .filter((value) => Number.isInteger(value) && value > 0)
            )];

            const quantityColumn = await this._getBomQuantityColumn();
            const partColumnSet = await this._getPartColumnSet();
            const tricomaColumn = this._getTricomaColumnFromPartColumnSet(partColumnSet);
            const partStockSql = await this._buildPartStockSnapshotSql();

            const bomRows = productIds.length === 0
                ? []
                : await this._exec(
                    `
                        SELECT
                            b.product_id,
                            b.part_id,
                            COALESCE(parts.part_name, CONCAT('Part ', b.part_id)) AS part_name,
                            parts.${mysql.escapeId(tricomaColumn)} AS tricoma_nr,
                            b.${mysql.escapeId(quantityColumn)} AS quantity_per_product,
                            COALESCE(stock.tracked_stock, 0) AS tracked_stock
                        FROM bom b
                        LEFT JOIN parts ON b.part_id = parts.part_id
                        LEFT JOIN (${partStockSql}) stock ON b.part_id = stock.part_id
                        WHERE b.product_id IN (${productIds.map(() => '?').join(', ')})
                        ORDER BY b.product_id, parts.part_name, b.part_id;
                    `,
                    productIds
                );

            const requirementsByProductId = new Map();
            const remainingStockByPartId = new Map();

            bomRows.forEach((row) => {
                const productId = Number(row?.product_id);
                const partId = Number(row?.part_id);
                const requirement = {
                    part_id: Number.isInteger(partId) && partId > 0 ? partId : null,
                    part_name: String(row?.part_name || '').trim() || `Part ${row?.part_id || ''}`,
                    tricoma_nr: row?.tricoma_nr == null ? '' : String(row.tricoma_nr),
                    quantity_per_product: Number(row?.quantity_per_product ?? 0),
                    tracked_stock: Number(row?.tracked_stock ?? 0)
                };

                if (!requirementsByProductId.has(productId)) {
                    requirementsByProductId.set(productId, []);
                }
                requirementsByProductId.get(productId).push(requirement);

                if (requirement.part_id != null && !remainingStockByPartId.has(requirement.part_id)) {
                    remainingStockByPartId.set(requirement.part_id, requirement.tracked_stock);
                }
            });

            const orders = [];
            let allocatedOrders = 0;
            let shortageOrders = 0;
            let scheduledUnits = 0;

            orderRows.forEach((row) => {
                const productionScheduleId = Number(row?.production_schedule_id);
                const productId = Number(row?.product_id);
                const plannedQuantity = Number(row?.planned_quantity ?? 0);
                const dueDate = row?.due_date instanceof Date
                    ? row.due_date.toISOString().slice(0, 10)
                    : String(row?.due_date || '').slice(0, 10);
                const scheduleStatus = String(row?.status || 'pending').trim().toLowerCase() || 'pending';
                const comments = String(row?.comments || '').trim();

                if (scheduleStatus === 'completed') {
                    return;
                }

                const baseRequirements = requirementsByProductId.get(productId) || [];
                const allocationMaterials = baseRequirements.map((requirement) => {
                    const partId = requirement.part_id;
                    const availableBeforeOrder = partId == null
                        ? 0
                        : Math.max(Number(remainingStockByPartId.get(partId) ?? 0), 0);
                    const requiredQuantity = Number(requirement.quantity_per_product ?? 0) * plannedQuantity;
                    const reservedQuantity = Math.min(availableBeforeOrder, requiredQuantity);
                    const shortageQuantity = Math.max(requiredQuantity - availableBeforeOrder, 0);
                    const remainingAfterReservation = Math.max(availableBeforeOrder - requiredQuantity, 0);

                    if (partId != null) {
                        remainingStockByPartId.set(partId, remainingAfterReservation);
                    }

                    return {
                        part_id: partId,
                        part_name: requirement.part_name,
                        tricoma_nr: requirement.tricoma_nr,
                        quantity_per_product: Number(requirement.quantity_per_product ?? 0),
                        required_quantity: requiredQuantity,
                        available_before_order: availableBeforeOrder,
                        reserved_quantity: reservedQuantity,
                        shortage_quantity: shortageQuantity,
                        remaining_after_reservation: remainingAfterReservation,
                        has_enough_reserved: shortageQuantity <= 0
                    };
                });

                const missingPartsCount = allocationMaterials.filter((material) => Number(material.shortage_quantity) > 0).length;
                const totalShortageQuantity = allocationMaterials.reduce((sum, material) => sum + Number(material.shortage_quantity || 0), 0);
                const hasBom = allocationMaterials.length > 0;
                const canAllocateFully = hasBom && missingPartsCount === 0;
                const allocationStatus = !hasBom
                    ? 'No BOM'
                    : canAllocateFully
                        ? 'Allocated'
                        : 'Shortage';

                if (canAllocateFully) {
                    allocatedOrders += 1;
                } else {
                    shortageOrders += 1;
                }

                scheduledUnits += plannedQuantity;
                orders.push({
                    production_schedule_id: productionScheduleId,
                    product_id: productId,
                    product_name: String(row?.product_name || '').trim() || `Product ${row?.product_id || ''}`,
                    planned_quantity: plannedQuantity,
                    due_date: dueDate,
                    comments,
                    schedule_status: scheduleStatus,
                    created_at: row?.created_at,
                    created_by_user_id: row?.created_by_user_id == null ? null : Number(row.created_by_user_id),
                    created_by: String(row?.created_by || ''),
                    completed_at: row?.completed_at || null,
                    completed_by_user_id: row?.completed_by_user_id == null ? null : Number(row.completed_by_user_id),
                    completed_by: String(row?.completed_by || '').trim(),
                    fin_product_id: Number(row?.fin_product_id) > 0 ? Number(row.fin_product_id) : null,
                    total_materials: allocationMaterials.length,
                    missing_parts_count: missingPartsCount,
                    total_shortage_quantity: totalShortageQuantity,
                    can_allocate_fully: canAllocateFully,
                    allocation_status: allocationStatus,
                    allocation_materials: allocationMaterials
                });
            });

            return {
                orders,
                meta: {
                    pending_orders: pendingRows.length,
                    scheduled_units: scheduledUnits,
                    allocated_orders: allocatedOrders,
                    shortage_orders: shortageOrders,
                    next_due_date: pendingRows[0]
                        ? (pendingRows[0]?.due_date instanceof Date
                            ? pendingRows[0].due_date.toISOString().slice(0, 10)
                            : String(pendingRows[0]?.due_date || '').slice(0, 10))
                        : ''
                }
            };
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    async getProductionScheduleOrderById(production_schedule_id) {
        try {
            const rows = await this._exec(
                `
                    SELECT
                        pso.production_schedule_id,
                        pso.product_id,
                        prod.product_name,
                        pso.planned_quantity,
                        pso.due_date,
                        pso.comments,
                        pso.status,
                        pso.completed_at,
                        pso.completed_by_user_id,
                        COALESCE(completed_user.display_name, completed_user.username, pso.completed_by_username, '') AS completed_by,
                        pso.fin_product_id,
                        pso.created_by_user_id,
                        COALESCE(u.display_name, u.username, pso.created_by_username, 'Legacy') AS created_by,
                        pso.created_at
                    FROM production_schedule_orders pso
                    LEFT JOIN products prod ON pso.product_id = prod.product_id
                    LEFT JOIN users u ON pso.created_by_user_id = u.user_id
                    LEFT JOIN users completed_user ON pso.completed_by_user_id = completed_user.user_id
                    WHERE pso.production_schedule_id = ?
                    LIMIT 1;
                `,
                [production_schedule_id]
            );

            const row = rows?.[0];
            if (!row) {
                return null;
            }

            return {
                production_schedule_id: Number(row.production_schedule_id),
                product_id: Number(row.product_id),
                product_name: String(row.product_name || '').trim(),
                planned_quantity: Number(row.planned_quantity || 0),
                due_date: row?.due_date instanceof Date
                    ? row.due_date.toISOString().slice(0, 10)
                    : String(row?.due_date || '').slice(0, 10),
                comments: String(row.comments || '').trim(),
                status: String(row.status || 'pending').trim().toLowerCase(),
                created_at: row.created_at,
                created_by_user_id: row.created_by_user_id == null ? null : Number(row.created_by_user_id),
                created_by: String(row.created_by || ''),
                completed_at: row.completed_at || null,
                completed_by_user_id: row.completed_by_user_id == null ? null : Number(row.completed_by_user_id),
                completed_by: String(row.completed_by || '').trim(),
                fin_product_id: Number(row?.fin_product_id) > 0 ? Number(row.fin_product_id) : null
            };
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    async insertProductionScheduleOrder(product_id, planned_quantity, due_date, comments = '', actor = null) {
        try {
            const result = await this._exec(
                `
                    INSERT INTO production_schedule_orders (
                        product_id,
                        planned_quantity,
                        due_date,
                        comments,
                        created_by_user_id,
                        created_by_username
                    )
                    VALUES (?, ?, ?, ?, ?, ?);
                `,
                [
                    product_id,
                    planned_quantity,
                    due_date,
                    comments || null,
                    actor?.user_id || null,
                    actor?.username || null
                ]
            );

            return this.getProductionScheduleOrderById(result.insertId);
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    async _insertFinishedProductOnConnection(connection, product_id, quantity, actor = null) {
        const exec = (query, params = []) => this._execOnConnection(connection, query, params);
        const insertResult = await exec(
            `
                INSERT INTO finished_products (
                    product_id,
                    fin_product_qty,
                    fin_product_date,
                    created_by_user_id,
                    created_by_username
                )
                VALUES (?, ?, NOW(), ?, ?);
            `,
            [product_id, quantity, actor?.user_id || null, actor?.username || null]
        );

        const finishedProductRows = await exec(
            `
                SELECT
                    fin_product_id,
                    product_id,
                    fin_product_qty,
                    fin_product_date,
                    created_by_user_id,
                    created_by_username
                FROM finished_products
                WHERE fin_product_id = ?
                LIMIT 1;
            `,
            [insertResult.insertId]
        );

        const finishedProduct = finishedProductRows?.[0];
        await this._createProductionRunSnapshotOnConnection(connection, {
            fin_product_id: Number(finishedProduct?.fin_product_id),
            product_id: Number(finishedProduct?.product_id ?? product_id),
            quantity: Number(finishedProduct?.fin_product_qty ?? quantity),
            production_date: finishedProduct?.fin_product_date || new Date(),
            created_by_user_id: finishedProduct?.created_by_user_id == null ? null : Number(finishedProduct.created_by_user_id),
            created_by_username: finishedProduct?.created_by_username == null ? null : String(finishedProduct.created_by_username)
        });

        return {
            fin_product_id: insertResult.insertId,
            product_id,
            fin_product_qty: quantity,
            fin_product_date: finishedProduct?.fin_product_date || new Date().toISOString(),
            created_by: actor?.username || null
        };
    }

    async completeProductionScheduleOrder(production_schedule_id, actor = null) {
        return this._withTransaction(async (connection) => {
            const exec = (query, params = []) => this._execOnConnection(connection, query, params);
            const scheduleRows = await exec(
                `
                    SELECT
                        production_schedule_id,
                        product_id,
                        planned_quantity,
                        due_date,
                        comments,
                        status,
                        fin_product_id
                    FROM production_schedule_orders
                    WHERE production_schedule_id = ?
                    LIMIT 1
                    FOR UPDATE;
                `,
                [production_schedule_id]
            );

            const scheduleOrder = scheduleRows?.[0];
            if (!scheduleOrder) {
                return {
                    order_found: false,
                    order: null,
                    finished_product: null
                };
            }

            const status = String(scheduleOrder.status || 'pending').trim().toLowerCase();
            if (status !== 'pending') {
                return {
                    order_found: true,
                    already_completed: status === 'completed',
                    order: scheduleOrder,
                    finished_product: null
                };
            }

            const finishedProduct = await this._insertFinishedProductOnConnection(
                connection,
                Number(scheduleOrder.product_id),
                Number(scheduleOrder.planned_quantity),
                actor
            );

            await exec(
                `
                    UPDATE production_schedule_orders
                    SET
                        status = 'completed',
                        completed_at = NOW(),
                        completed_by_user_id = ?,
                        completed_by_username = ?,
                        fin_product_id = ?
                    WHERE production_schedule_id = ?;
                `,
                [
                    actor?.user_id || null,
                    actor?.username || null,
                    finishedProduct.fin_product_id,
                    production_schedule_id
                ]
            );

            return {
                order_found: true,
                already_completed: false,
                finished_product: finishedProduct,
                order: {
                    production_schedule_id: Number(scheduleOrder.production_schedule_id),
                    product_id: Number(scheduleOrder.product_id),
                    planned_quantity: Number(scheduleOrder.planned_quantity),
                    due_date: scheduleOrder?.due_date instanceof Date
                        ? scheduleOrder.due_date.toISOString().slice(0, 10)
                        : String(scheduleOrder?.due_date || '').slice(0, 10),
                    comments: String(scheduleOrder?.comments || '').trim()
                }
            };
        });
    }

    async deleteProductionScheduleOrder(production_schedule_id) {
        try {
            return await this._exec(
                `
                    DELETE FROM production_schedule_orders
                    WHERE production_schedule_id = ?;
                `,
                [production_schedule_id]
            );
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // ------------------------------------------------------------------------
    // Section 5: Finished Products and Stock Movement History
    // ------------------------------------------------------------------------
    async _buildAssemblyConsumptionHistorySql() {
        const quantityColumn = await this._getBomQuantityColumn();
        const partColumnSet = await this._getPartColumnSet();
        const tricomaColumn = this._getTricomaColumnFromPartColumnSet(partColumnSet);

        return `
            SELECT
                pr.production_run_id,
                pr.fin_product_id,
                pr.product_id,
                pr.product_name_snapshot AS product_name,
                pr.production_date,
                pr.created_by_user_id,
                pr.created_by_username,
                prm.part_id,
                prm.part_name_snapshot AS part_name,
                prm.tricoma_nr_snapshot AS tricoma_nr,
                prm.quantity_per_product_snapshot AS quantity_per_product,
                prm.quantity_used
            FROM production_run_materials prm
            INNER JOIN production_runs pr ON prm.production_run_id = pr.production_run_id

            UNION ALL

            SELECT
                -fp.fin_product_id AS production_run_id,
                fp.fin_product_id,
                fp.product_id,
                COALESCE(prod.product_name, CONCAT('Product ', fp.product_id)) AS product_name,
                fp.fin_product_date AS production_date,
                fp.created_by_user_id,
                fp.created_by_username,
                b.part_id,
                COALESCE(p.part_name, CONCAT('Part ', b.part_id)) AS part_name,
                p.${mysql.escapeId(tricomaColumn)} AS tricoma_nr,
                b.${mysql.escapeId(quantityColumn)} AS quantity_per_product,
                (fp.fin_product_qty * b.${mysql.escapeId(quantityColumn)}) AS quantity_used
            FROM finished_products fp
            INNER JOIN bom b ON fp.product_id = b.product_id
            LEFT JOIN parts p ON b.part_id = p.part_id
            LEFT JOIN products prod ON fp.product_id = prod.product_id
            LEFT JOIN production_runs pr ON pr.fin_product_id = fp.fin_product_id
            WHERE pr.production_run_id IS NULL
        `;
    }

    async _buildPartStockSnapshotSql() {
        const assemblySql = await this._buildAssemblyConsumptionHistorySql();

        return `
            SELECT
                p.part_id,
                p.part_name,
                COALESCE(SUM(l.qty), 0) AS tracked_stock,
                COALESCE(SUM(CASE WHEN l.source = 'movement' THEN l.qty ELSE 0 END), 0) AS total_movements,
                COALESCE(SUM(CASE WHEN l.source = 'assembly' THEN -l.qty ELSE 0 END), 0) AS total_used_in_assembly,
                COALESCE(p.reorder_level, 0) AS reorder_level,
                COALESCE(p.reorder_quantity, 0) AS reorder_quantity
            FROM parts p
            LEFT JOIN (
                SELECT
                    sm.part_id,
                    sm.stock_movement_qty AS qty,
                    'movement' AS source
                FROM stock_movement sm

                UNION ALL

                SELECT
                    assembly.part_id,
                    -assembly.quantity_used AS qty,
                    'assembly' AS source
                FROM (${assemblySql}) assembly
            ) l ON p.part_id = l.part_id
            GROUP BY
                p.part_id,
                p.part_name,
                p.reorder_level,
                p.reorder_quantity
        `;
    }

    // Returns finished product history joined to product names.
    async getFinishedProducts() {
        try {
            return await this._exec(
                `
                    SELECT
                        fp.fin_product_id,
                        p.product_name,
                        fp.fin_product_qty,
                        fp.fin_product_date,
                        COALESCE(fp.created_by_username, u.display_name, u.username, 'Legacy') AS created_by,
                        pso.production_schedule_id,
                        pso.due_date,
                        pso.comments
                    FROM finished_products fp
                    LEFT JOIN products p ON fp.product_id = p.product_id
                    LEFT JOIN users u ON fp.created_by_user_id = u.user_id
                    LEFT JOIN production_schedule_orders pso ON pso.fin_product_id = fp.fin_product_id
                    ORDER BY fp.fin_product_id;
                `
            );
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Searches finished products by product name and formatted dates.
    async searchFinishedProducts(queryText) {
        try {
            return await this._exec(
                `
                    SELECT
                        fp.fin_product_id,
                        p.product_name,
                        fp.fin_product_qty,
                        fp.fin_product_date,
                        COALESCE(fp.created_by_username, u.display_name, u.username, 'Legacy') AS created_by,
                        pso.production_schedule_id,
                        pso.due_date,
                        pso.comments
                    FROM finished_products fp
                    LEFT JOIN products p ON fp.product_id = p.product_id
                    LEFT JOIN users u ON fp.created_by_user_id = u.user_id
                    LEFT JOIN production_schedule_orders pso ON pso.fin_product_id = fp.fin_product_id
                    WHERE p.product_name LIKE ?
                       OR DATE_FORMAT(fp.fin_product_date, '%d/%m/%y') LIKE ?
                       OR DATE_FORMAT(fp.fin_product_date, '%d/%m/%Y') LIKE ?
                       OR COALESCE(fp.created_by_username, u.display_name, u.username, '') LIKE ?
                       OR COALESCE(CAST(pso.production_schedule_id AS CHAR), '') LIKE ?
                       OR DATE_FORMAT(pso.due_date, '%d/%m/%y') LIKE ?
                       OR DATE_FORMAT(pso.due_date, '%d/%m/%Y') LIKE ?
                       OR COALESCE(pso.comments, '') LIKE ?
                    ORDER BY fp.fin_product_id;
                `,
                [`%${queryText}%`, `%${queryText}%`, `%${queryText}%`, `%${queryText}%`, `%${queryText}%`, `%${queryText}%`, `%${queryText}%`, `%${queryText}%`]
            );
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Inserts one finished-product history row for assembly events.
    async insertFinishedProduct(product_id, quantity, actor = null) {
        return this._withTransaction(async (connection) => {
            return this._insertFinishedProductOnConnection(connection, product_id, quantity, actor);
        });
    }

    // Deletes one finished-product history entry.
    async deleteFinishedProduct(fin_product_id) {
        try {
            return await this._exec(
                `
                    DELETE FROM finished_products
                    WHERE fin_product_id = ?;
                `,
                [fin_product_id]
            );
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Returns yearly part-consumption aggregates based on frozen production snapshots.
    async getMostUsedPartsByYear(year) {
        try {
            return await this._exec(
                `
                    SELECT
                        prm.part_id,
                        prm.part_name_snapshot AS part_name,
                        prm.tricoma_nr_snapshot AS tricoma_nr,
                        SUM(prm.quantity_used) AS total_quantity_used,
                        COUNT(DISTINCT prm.production_run_id) AS production_runs
                    FROM production_run_materials prm
                    INNER JOIN production_runs pr ON prm.production_run_id = pr.production_run_id
                    WHERE YEAR(pr.production_date) = ?
                    GROUP BY prm.part_id, prm.part_name_snapshot, prm.tricoma_nr_snapshot
                    ORDER BY total_quantity_used DESC, prm.part_name_snapshot ASC;
                `,
                [year]
            );
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Returns the initial dashboard KPI snapshot used by the landing screen.
    async getDashboardKpis({ year, month }) {
        try {
            const partStockSql = await this._buildPartStockSnapshotSql();
            const partColumnSet = await this._getPartColumnSet();
            const tricomaColumn = this._getTricomaColumnFromPartColumnSet(partColumnSet);
            const [
                monthlyProductionRows,
                topUsedPartRows,
                topProducedProductRows,
                shortagePartRows,
                pendingProductionRows
            ] = await Promise.all([
                this._exec(
                    `
                        SELECT COALESCE(SUM(pr.produced_quantity), 0) AS total_quantity_produced
                        FROM production_runs pr
                        WHERE YEAR(pr.production_date) = ?
                          AND MONTH(pr.production_date) = ?;
                    `,
                    [year, month]
                ),
                this._exec(
                    `
                        SELECT
                            prm.part_id,
                            prm.part_name_snapshot AS part_name,
                            prm.tricoma_nr_snapshot AS tricoma_nr,
                            SUM(prm.quantity_used) AS total_quantity_used
                        FROM production_run_materials prm
                        INNER JOIN production_runs pr ON prm.production_run_id = pr.production_run_id
                        WHERE YEAR(pr.production_date) = ?
                        GROUP BY prm.part_id, prm.part_name_snapshot, prm.tricoma_nr_snapshot
                        ORDER BY total_quantity_used DESC, prm.part_name_snapshot ASC
                        LIMIT 1;
                    `,
                    [year]
                ),
                this._exec(
                    `
                        SELECT
                            pr.product_id,
                            pr.product_name_snapshot AS product_name,
                            SUM(pr.produced_quantity) AS total_quantity_produced
                        FROM production_runs pr
                        WHERE YEAR(pr.production_date) = ?
                        GROUP BY pr.product_id, pr.product_name_snapshot
                        ORDER BY total_quantity_produced DESC, pr.product_name_snapshot ASC
                        LIMIT 1;
                    `,
                    [year]
                ),
                this._exec(
                    `
                        SELECT
                            v.part_id,
                            p.part_name,
                            p.${mysql.escapeId(tricomaColumn)} AS tricoma_nr,
                            COALESCE(v.tracked_stock, 0) AS tracked_stock,
                            COALESCE(v.reorder_level, 0) AS reorder_level,
                            GREATEST(COALESCE(v.reorder_level, 0) - COALESCE(v.tracked_stock, 0), 0) AS missing_quantity
                        FROM (${partStockSql}) v
                        INNER JOIN parts p ON p.part_id = v.part_id
                        WHERE COALESCE(v.tracked_stock, 0) < COALESCE(v.reorder_level, 0)
                        ORDER BY missing_quantity DESC, p.part_name ASC
                        LIMIT 10;
                    `
                ),
                this._exec(
                    `
                        SELECT
                            pso.production_schedule_id,
                            pso.product_id,
                            COALESCE(prod.product_name, CONCAT('Product ', pso.product_id)) AS product_name,
                            pso.planned_quantity,
                            pso.due_date,
                            pso.comments,
                            COALESCE(u.display_name, u.username, pso.created_by_username, 'Legacy') AS created_by
                        FROM production_schedule_orders pso
                        LEFT JOIN products prod ON pso.product_id = prod.product_id
                        LEFT JOIN users u ON pso.created_by_user_id = u.user_id
                        WHERE LOWER(COALESCE(pso.status, 'pending')) = 'pending'
                        ORDER BY pso.due_date ASC, pso.created_at ASC, pso.production_schedule_id ASC
                        LIMIT 5;
                    `
                )
            ]);

            const topUsedPart = topUsedPartRows?.[0]
                ? {
                    part_id: Number(topUsedPartRows[0].part_id || 0),
                    part_name: String(topUsedPartRows[0].part_name || ''),
                    tricoma_nr: topUsedPartRows[0].tricoma_nr == null ? '' : String(topUsedPartRows[0].tricoma_nr),
                    total_quantity_used: Number(topUsedPartRows[0].total_quantity_used || 0)
                }
                : null;

            const topProducedProduct = topProducedProductRows?.[0]
                ? {
                    product_id: Number(topProducedProductRows[0].product_id || 0),
                    product_name: String(topProducedProductRows[0].product_name || ''),
                    total_quantity_produced: Number(topProducedProductRows[0].total_quantity_produced || 0)
                }
                : null;

            const shortageParts = Array.isArray(shortagePartRows)
                ? shortagePartRows.map((row) => ({
                    part_id: Number(row?.part_id || 0),
                    part_name: String(row?.part_name || ''),
                    tricoma_nr: row?.tricoma_nr == null ? '' : String(row.tricoma_nr),
                    tracked_stock: Number(row?.tracked_stock || 0),
                    reorder_level: Number(row?.reorder_level || 0),
                    missing_quantity: Number(row?.missing_quantity || 0)
                }))
                : [];

            const pendingProductions = Array.isArray(pendingProductionRows)
                ? pendingProductionRows.map((row) => ({
                    production_schedule_id: Number(row?.production_schedule_id || 0),
                    product_id: Number(row?.product_id || 0),
                    product_name: String(row?.product_name || ''),
                    planned_quantity: Number(row?.planned_quantity || 0),
                    due_date: row?.due_date instanceof Date
                        ? row.due_date.toISOString().slice(0, 10)
                        : String(row?.due_date || '').slice(0, 10),
                    comments: String(row?.comments || '').trim(),
                    created_by: String(row?.created_by || '')
                }))
                : [];

            return {
                production_this_month: Number(monthlyProductionRows?.[0]?.total_quantity_produced || 0),
                top_used_part: topUsedPart,
                top_produced_product: topProducedProduct,
                shortage_parts: shortageParts,
                pending_productions: pendingProductions
            };
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Returns yearly production aggregates grouped by product snapshot.
    async getMostProducedProductsByYear(year) {
        try {
            return await this._exec(
                `
                    SELECT
                        pr.product_id,
                        pr.product_name_snapshot AS product_name,
                        SUM(pr.produced_quantity) AS total_quantity_produced,
                        COUNT(*) AS production_runs
                    FROM production_runs pr
                    WHERE YEAR(pr.production_date) = ?
                    GROUP BY pr.product_id, pr.product_name_snapshot
                    ORDER BY total_quantity_produced DESC, pr.product_name_snapshot ASC;
                `,
                [year]
            );
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Builds one unified transaction-history query that combines manual stock
    // movements with the derived material consumption caused by finished-product
    // assembly. This keeps the audit trail readable without double-counting stock.
    async _buildStockTransactionHistorySql() {
        const assemblySql = await this._buildAssemblyConsumptionHistorySql();

        return `
            SELECT
                tx.stock_movement_id,
                tx.part_name,
                tx.stock_movement_qty,
                tx.stock_movement_date,
                tx.created_by,
                tx.transaction_source,
                tx.movement_type,
                tx.source_product_name,
                tx.can_delete
            FROM (
                SELECT
                    sm.stock_movement_id,
                    COALESCE(p.part_name, CONCAT('Part ', sm.part_id)) AS part_name,
                    sm.stock_movement_qty,
                    sm.stock_movement_date,
                    COALESCE(sm.created_by_username, u.display_name, u.username, 'Legacy') AS created_by,
                    'movement' AS transaction_source,
                    CASE
                        WHEN sm.stock_movement_qty > 0 THEN 'IN'
                        WHEN sm.stock_movement_qty < 0 THEN 'OUT'
                        ELSE '0'
                    END AS movement_type,
                    NULL AS source_product_name,
                    1 AS can_delete
                FROM stock_movement sm
                LEFT JOIN parts p ON sm.part_id = p.part_id
                LEFT JOIN users u ON sm.created_by_user_id = u.user_id

                UNION ALL

                SELECT
                    -((ABS(assembly.production_run_id) * 1000000) + COALESCE(assembly.part_id, 0)) AS stock_movement_id,
                    assembly.part_name AS part_name,
                    -assembly.quantity_used AS stock_movement_qty,
                    assembly.production_date AS stock_movement_date,
                    COALESCE(assembly.created_by_username, u.display_name, u.username, 'Legacy') AS created_by,
                    'assembly' AS transaction_source,
                    'OUT - Production' AS movement_type,
                    assembly.product_name AS source_product_name,
                    0 AS can_delete
                FROM (${assemblySql}) assembly
                LEFT JOIN users u ON assembly.created_by_user_id = u.user_id
            ) tx
        `;
    }

    _buildStockMovementWhereClause(filters = {}) {
        const {
            query = '',
            movement_type = '',
            created_by = '',
            source_product_name = '',
            date_from = '',
            date_to = ''
        } = filters;
        const whereClauses = [];
        const params = [];

        if (query) {
            whereClauses.push(`
                (
                    tx.part_name LIKE ?
                    OR COALESCE(tx.source_product_name, '') LIKE ?
                    OR DATE_FORMAT(tx.stock_movement_date, '%d/%m/%y') LIKE ?
                    OR DATE_FORMAT(tx.stock_movement_date, '%d/%m/%Y') LIKE ?
                    OR tx.created_by LIKE ?
                    OR tx.movement_type LIKE ?
                )
            `);
            params.push(`%${query}%`, `%${query}%`, `%${query}%`, `%${query}%`, `%${query}%`, `%${query}%`);
        }

        if (movement_type) {
            whereClauses.push('tx.movement_type = ?');
            params.push(movement_type);
        }

        if (created_by) {
            whereClauses.push('tx.created_by = ?');
            params.push(created_by);
        }

        if (source_product_name) {
            whereClauses.push('COALESCE(tx.source_product_name, \'\') = ?');
            params.push(source_product_name);
        }

        if (date_from) {
            whereClauses.push('DATE(tx.stock_movement_date) >= ?');
            params.push(date_from);
        }

        if (date_to) {
            whereClauses.push('DATE(tx.stock_movement_date) <= ?');
            params.push(date_to);
        }

        return {
            whereClause: whereClauses.length > 0 ? `WHERE ${whereClauses.join('\n  AND ')}` : '',
            params
        };
    }

    // Returns stock movement history joined to part names.
    async getStockMovements(filters = {}) {
        try {
            const sql = await this._buildStockTransactionHistorySql();
            const { whereClause, params } = this._buildStockMovementWhereClause(filters);
            return await this._exec(`
                ${sql}
                ${whereClause}
                ORDER BY tx.stock_movement_date, tx.stock_movement_id;
            `, params);
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Searches the combined transaction history by part, product, user, type, and date.
    async searchStockMovements(queryText, filters = {}) {
        try {
            return await this.getStockMovements({
                ...filters,
                query: queryText
            });
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Returns distinct values used to populate the Transactions filter controls.
    async getStockMovementFilterOptions() {
        try {
            const sql = await this._buildStockTransactionHistorySql();
            const [users, sourceProducts] = await Promise.all([
                this._exec(
                    `
                        ${sql}
                        WHERE tx.created_by IS NOT NULL
                          AND tx.created_by <> ''
                        GROUP BY tx.created_by
                        ORDER BY tx.created_by;
                    `
                ),
                this._exec(
                    `
                        ${sql}
                        WHERE tx.source_product_name IS NOT NULL
                          AND tx.source_product_name <> ''
                        GROUP BY tx.source_product_name
                        ORDER BY tx.source_product_name;
                    `
                )
            ]);

            return {
                movement_types: ['IN', 'OUT', 'OUT - Production'],
                created_by: users.map((row) => String(row.created_by)),
                source_products: sourceProducts.map((row) => String(row.source_product_name))
            };
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Inserts one stock movement row (positive = IN, negative = OUT).
    async insertStockMovement(part_id, quantity, actor = null) {
        try {
            const result = await this._exec(
                `
                    INSERT INTO stock_movement (
                        part_id,
                        stock_movement_qty,
                        stock_movement_date,
                        created_by_user_id,
                        created_by_username
                    )
                    VALUES (?, ?, NOW(), ?, ?);
                `,
                [part_id, quantity, actor?.user_id || null, actor?.username || null]
            );

            return {
                stock_movement_id: result.insertId,
                part_id,
                stock_movement_qty: quantity,
                stock_movement_date: new Date().toISOString(),
                created_by: actor?.username || null
            };
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // Deletes one stock movement history entry.
    async deleteStockMovement(stock_movement_id) {
        try {
            return await this._exec(
                `
                    DELETE FROM stock_movement
                    WHERE stock_movement_id = ?;
                `,
                [stock_movement_id]
            );
        } catch (error) {
            console.log(error);
            throw error;
        }
    }

    // ------------------------------------------------------------------------
    // Section 6: Current Stock Snapshot
    // ------------------------------------------------------------------------
    // Returns the stock snapshot used by the Current Stock UI table.
    // Tricoma value is joined from `parts` because it is not exposed by vw_part_stock.
    async getPartStock() {
        try {
            const partColumnSet = await this._getPartColumnSet();
            const tricomaColumn = this._getTricomaColumnFromPartColumnSet(partColumnSet);
            const partStockSql = await this._buildPartStockSnapshotSql();

            // The stock view is intentionally read-only and denormalized: the frontend
            // gets everything it needs for filtering, highlighting, and PDF export in one payload.
            return await this._exec(
                `
                    SELECT
                        v.part_id,
                        v.part_name,
                        p.${mysql.escapeId(tricomaColumn)} AS tricoma_nr,
                        v.tracked_stock,
                        v.total_movements,
                        v.total_used_in_assembly,
                        v.reorder_level,
                        v.reorder_quantity
                    FROM (${partStockSql}) v
                    LEFT JOIN parts p ON v.part_id = p.part_id;
                `
            );
        } catch (error) {
            console.log(error);
            throw error;
        }
    }
}

module.exports = DbService;
