-- ============================================================================
-- MariaDB First-Boot Schema Initialization
-- ============================================================================
-- Purpose:
-- - Create the core inventory tables automatically when the DB volume is empty.
-- - Keep this file limited to the base catalog/inventory schema.
-- - Leave auth, audit, logs, and production snapshot tables to the app startup.
--
-- This file is executed automatically by the MariaDB container only on the
-- first startup of a fresh `db_data` volume via `/docker-entrypoint-initdb.d`.
-- ============================================================================

SET NAMES utf8mb4;

CREATE TABLE IF NOT EXISTS products (
    product_id INT NOT NULL AUTO_INCREMENT,
    product_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (product_id),
    KEY idx_products_product_name (product_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS parts (
    part_id INT NOT NULL AUTO_INCREMENT,
    part_name VARCHAR(255) NOT NULL,
    stock_level DECIMAL(18,4) NOT NULL DEFAULT 0.0000,
    reorder_level DECIMAL(18,4) NOT NULL DEFAULT 0.0000,
    reorder_quantity DECIMAL(18,4) NOT NULL DEFAULT 0.0000,
    tricoma_nr VARCHAR(120) NULL,
    PRIMARY KEY (part_id),
    KEY idx_parts_part_name (part_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS bom (
    bom_id INT NOT NULL AUTO_INCREMENT,
    product_id INT NOT NULL,
    part_id INT NOT NULL,
    bom_qty DECIMAL(18,4) NOT NULL DEFAULT 0.0000,
    part_unit VARCHAR(100) NULL,
    PRIMARY KEY (bom_id),
    KEY idx_bom_product_id (product_id),
    KEY idx_bom_part_id (part_id),
    CONSTRAINT fk_bom_product
        FOREIGN KEY (product_id)
        REFERENCES products (product_id),
    CONSTRAINT fk_bom_part
        FOREIGN KEY (part_id)
        REFERENCES parts (part_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS finished_products (
    fin_product_id INT NOT NULL AUTO_INCREMENT,
    product_id INT NOT NULL,
    fin_product_qty DECIMAL(18,4) NOT NULL DEFAULT 0.0000,
    fin_product_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fin_product_id),
    KEY idx_finished_product_product_id (product_id),
    KEY idx_finished_product_date (fin_product_date),
    CONSTRAINT fk_finished_products_product
        FOREIGN KEY (product_id)
        REFERENCES products (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS stock_movement (
    stock_movement_id INT NOT NULL AUTO_INCREMENT,
    part_id INT NOT NULL,
    stock_movement_qty DECIMAL(18,4) NOT NULL DEFAULT 0.0000,
    stock_movement_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stock_movement_id),
    KEY idx_stock_movement_part_id (part_id),
    KEY idx_stock_movement_date (stock_movement_date),
    CONSTRAINT fk_stock_movement_part
        FOREIGN KEY (part_id)
        REFERENCES parts (part_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
