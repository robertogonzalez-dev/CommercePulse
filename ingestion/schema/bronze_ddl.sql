-- ============================================================
-- CommercePulse | Bronze Layer DDL
-- Schema: bronze
-- Purpose: Raw ingestion tables — immutable, full-fidelity.
--          Every table carries audit columns appended at load time.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS bronze;

-- ------------------------------------------------------------
-- bronze.raw_customers
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_customers (
    customer_id           VARCHAR,
    first_name            VARCHAR,
    last_name             VARCHAR,
    email                 VARCHAR,
    phone                 VARCHAR,
    gender                VARCHAR,
    date_of_birth         DATE,
    city                  VARCHAR,
    state                 VARCHAR,
    country               VARCHAR,
    zip_code              VARCHAR,
    acquisition_channel   VARCHAR,
    registration_date     TIMESTAMP,
    is_active             BOOLEAN,
    -- audit columns
    _ingested_at          TIMESTAMP NOT NULL,
    _batch_id             VARCHAR   NOT NULL,
    _source_file          VARCHAR   NOT NULL,
    _row_hash             VARCHAR   NOT NULL
);

-- ------------------------------------------------------------
-- bronze.raw_products
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_products (
    product_id     VARCHAR,
    product_name   VARCHAR,
    category_l1    VARCHAR,
    category_l2    VARCHAR,
    brand          VARCHAR,
    sku            VARCHAR,
    cost_price     DECIMAL(12, 4),
    list_price     DECIMAL(12, 4),
    weight_kg      DECIMAL(8,  3),
    is_active      BOOLEAN,
    created_date   DATE,
    -- audit columns
    _ingested_at   TIMESTAMP NOT NULL,
    _batch_id      VARCHAR   NOT NULL,
    _source_file   VARCHAR   NOT NULL,
    _row_hash      VARCHAR   NOT NULL
);

-- ------------------------------------------------------------
-- bronze.raw_orders
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_orders (
    order_id                    VARCHAR,
    customer_id                 VARCHAR,
    order_date                  TIMESTAMP,
    order_status                VARCHAR,
    channel                     VARCHAR,
    shipping_address_city       VARCHAR,
    shipping_address_state      VARCHAR,
    shipping_address_country    VARCHAR,
    shipping_address_zip        VARCHAR,
    promotion_code              VARCHAR,
    shipping_cost               DECIMAL(10, 2),
    estimated_delivery_date     DATE,
    actual_delivery_date        DATE,
    -- audit columns
    _ingested_at                TIMESTAMP NOT NULL,
    _batch_id                   VARCHAR   NOT NULL,
    _source_file                VARCHAR   NOT NULL,
    _row_hash                   VARCHAR   NOT NULL
);

-- ------------------------------------------------------------
-- bronze.raw_order_items
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_order_items (
    order_item_id    VARCHAR,
    order_id         VARCHAR,
    product_id       VARCHAR,
    quantity         INTEGER,
    unit_price       DECIMAL(12, 4),
    discount_amount  DECIMAL(12, 4),
    line_total       DECIMAL(12, 4),
    -- audit columns
    _ingested_at     TIMESTAMP NOT NULL,
    _batch_id        VARCHAR   NOT NULL,
    _source_file     VARCHAR   NOT NULL,
    _row_hash        VARCHAR   NOT NULL
);

-- ------------------------------------------------------------
-- bronze.raw_payments
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_payments (
    payment_id       VARCHAR,
    order_id         VARCHAR,
    payment_date     TIMESTAMP,
    payment_method   VARCHAR,
    payment_status   VARCHAR,
    amount           DECIMAL(12, 4),
    currency         VARCHAR,
    transaction_id   VARCHAR,
    gateway          VARCHAR,
    -- audit columns
    _ingested_at     TIMESTAMP NOT NULL,
    _batch_id        VARCHAR   NOT NULL,
    _source_file     VARCHAR   NOT NULL,
    _row_hash        VARCHAR   NOT NULL
);

-- ------------------------------------------------------------
-- bronze.raw_refunds
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_refunds (
    refund_id      VARCHAR,
    order_id       VARCHAR,
    order_item_id  VARCHAR,
    customer_id    VARCHAR,
    refund_date    DATE,
    refund_reason  VARCHAR,
    refund_amount  DECIMAL(12, 4),
    refund_status  VARCHAR,
    refund_method  VARCHAR,
    -- audit columns
    _ingested_at   TIMESTAMP NOT NULL,
    _batch_id      VARCHAR   NOT NULL,
    _source_file   VARCHAR   NOT NULL,
    _row_hash      VARCHAR   NOT NULL
);

-- ------------------------------------------------------------
-- bronze.raw_inventory
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_inventory (
    inventory_id        VARCHAR,
    product_id          VARCHAR,
    warehouse_id        VARCHAR,
    warehouse_name      VARCHAR,
    quantity_on_hand    INTEGER,
    quantity_reserved   INTEGER,
    quantity_available  INTEGER,
    reorder_level       INTEGER,
    reorder_quantity    INTEGER,
    last_updated        DATE,
    -- audit columns
    _ingested_at        TIMESTAMP NOT NULL,
    _batch_id           VARCHAR   NOT NULL,
    _source_file        VARCHAR   NOT NULL,
    _row_hash           VARCHAR   NOT NULL
);

-- ------------------------------------------------------------
-- bronze.raw_marketing_spend
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_marketing_spend (
    spend_id            VARCHAR,
    campaign_id         VARCHAR,
    campaign_name       VARCHAR,
    channel             VARCHAR,
    start_date          DATE,
    end_date            DATE,
    budget              DECIMAL(14, 2),
    amount_spent        DECIMAL(14, 2),
    impressions         INTEGER,
    clicks              INTEGER,
    conversions         INTEGER,
    revenue_attributed  DECIMAL(14, 2),
    -- audit columns
    _ingested_at        TIMESTAMP NOT NULL,
    _batch_id           VARCHAR   NOT NULL,
    _source_file        VARCHAR   NOT NULL,
    _row_hash           VARCHAR   NOT NULL
);

-- ------------------------------------------------------------
-- bronze.raw_web_sessions
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_web_sessions (
    session_id         VARCHAR,
    customer_id        VARCHAR,
    session_start      TIMESTAMP,
    session_end        TIMESTAMP,
    channel            VARCHAR,
    device_type        VARCHAR,
    landing_page       VARCHAR,
    pages_viewed       INTEGER,
    products_viewed    INTEGER,
    cart_adds          INTEGER,
    checkout_started   INTEGER,
    order_id           VARCHAR,
    campaign_id        VARCHAR,
    utm_source                VARCHAR,
    utm_medium                VARCHAR,
    session_duration_seconds  DECIMAL(10, 2),  -- derived by WebSessionsLoader
    -- audit columns
    _ingested_at       TIMESTAMP NOT NULL,
    _batch_id          VARCHAR   NOT NULL,
    _source_file       VARCHAR   NOT NULL,
    _row_hash          VARCHAR   NOT NULL
);

-- ------------------------------------------------------------
-- bronze.ingestion_log  — pipeline run audit trail
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.ingestion_log (
    log_id          VARCHAR      NOT NULL,
    batch_id        VARCHAR      NOT NULL,
    dataset         VARCHAR      NOT NULL,
    source_file     VARCHAR      NOT NULL,
    target_table    VARCHAR      NOT NULL,
    load_type       VARCHAR      NOT NULL,
    status          VARCHAR      NOT NULL,  -- success | failed | skipped
    rows_read       INTEGER,
    rows_loaded     INTEGER,
    rows_rejected   INTEGER,
    error_message   VARCHAR,
    started_at      TIMESTAMP    NOT NULL,
    completed_at    TIMESTAMP,
    duration_seconds DECIMAL(10, 3),
    PRIMARY KEY (log_id)
);
