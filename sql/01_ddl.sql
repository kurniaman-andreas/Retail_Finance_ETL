

-- Drop existing tables to ensure clean 9-table schema setup
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_branch CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS fact_transaction CASCADE;

DROP TABLE IF EXISTS bronze_transactions CASCADE;
DROP TABLE IF EXISTS bronze_customers CASCADE;
DROP TABLE IF EXISTS bronze_branches CASCADE;

DROP TABLE IF EXISTS stg_transactions CASCADE;
DROP TABLE IF EXISTS stg_customers CASCADE;
DROP TABLE IF EXISTS stg_branches CASCADE;

DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS branches CASCADE;

-- ============================================================================
-- LAYER 1: BRONZE (RAW)
-- ============================================================================
CREATE TABLE bronze_transactions (
    transaction_id TEXT,
    customer_id TEXT,
    branch_id TEXT,
    transaction_date TEXT,
    amount TEXT,
    payment_method TEXT,
    transaction_status TEXT,
    channel TEXT,
    updated_at TEXT,
    merchant_category TEXT,
    device_type TEXT,
    currency TEXT,
    fraud_flag TEXT,
    promo_code TEXT,
    batch_date DATE NOT NULL
);

CREATE TABLE bronze_customers (
    customer_id TEXT,
    customer_name TEXT,
    city TEXT,
    registration_date TEXT,
    customer_status TEXT,
    customer_segment TEXT,
    email TEXT,
    phone_number TEXT,
    birth_date TEXT,
    occupation TEXT,
    income_band TEXT,
    kyc_status TEXT,
    batch_date DATE NOT NULL
);

CREATE TABLE bronze_branches (
    branch_id TEXT,
    branch_name TEXT,
    region TEXT,
    branch_status TEXT,
    branch_type TEXT,
    opening_date TEXT,
    manager_name TEXT,
    city TEXT,
    batch_date DATE NOT NULL
);

CREATE INDEX idx_bronze_transactions_batch_date ON bronze_transactions (batch_date);
CREATE INDEX idx_bronze_customers_batch_date ON bronze_customers (batch_date);
CREATE INDEX idx_bronze_branches_batch_date ON bronze_branches (batch_date);

-- ============================================================================
-- LAYER 2: STAGING (TRANSFORMED & DEDUPLICATED PER BATCH)
-- ============================================================================
CREATE TABLE stg_transactions (
    transaction_id TEXT NOT NULL,
    customer_id TEXT,
    branch_id TEXT,
    transaction_date TIMESTAMP,
    amount NUMERIC(18, 2),
    payment_method TEXT,
    transaction_status TEXT,
    channel TEXT,
    updated_at TIMESTAMP,
    merchant_category TEXT,
    device_type TEXT,
    currency TEXT,
    fraud_flag TEXT,
    promo_code TEXT,
    batch_date DATE NOT NULL,
    PRIMARY KEY (batch_date, transaction_id)
);

CREATE TABLE stg_customers (
    customer_id TEXT NOT NULL,
    customer_name TEXT,
    city TEXT,
    registration_date DATE,
    customer_status TEXT,
    customer_segment TEXT,
    email TEXT,
    phone_number TEXT,
    birth_date DATE,
    occupation TEXT,
    income_band TEXT,
    kyc_status TEXT,
    batch_date DATE NOT NULL,
    PRIMARY KEY (batch_date, customer_id)
);

CREATE TABLE stg_branches (
    branch_id TEXT NOT NULL,
    branch_name TEXT,
    region TEXT,
    branch_status TEXT,
    branch_type TEXT,
    opening_date DATE,
    manager_name TEXT,
    city TEXT,
    batch_date DATE NOT NULL,
    PRIMARY KEY (batch_date, branch_id)
);

-- ============================================================================
-- LAYER 3: CLEAN DWH (FINAL BUSINESS ENTITIES)
-- ============================================================================
CREATE TABLE transactions (
    transaction_id TEXT PRIMARY KEY,
    customer_id TEXT,
    branch_id TEXT,
    transaction_date TIMESTAMP,
    amount NUMERIC(18, 2),
    payment_method TEXT,
    transaction_status TEXT,
    channel TEXT,
    updated_at TIMESTAMP,
    merchant_category TEXT,
    device_type TEXT,
    currency TEXT,
    fraud_flag TEXT,
    promo_code TEXT,
    batch_date DATE NOT NULL
);

CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT,
    city TEXT,
    registration_date DATE,
    customer_status TEXT,
    customer_segment TEXT,
    email TEXT,
    phone_number TEXT,
    birth_date DATE,
    occupation TEXT,
    income_band TEXT,
    kyc_status TEXT,
    batch_date DATE NOT NULL
);

CREATE TABLE branches (
    branch_id TEXT PRIMARY KEY,
    branch_name TEXT,
    region TEXT,
    branch_status TEXT,
    branch_type TEXT,
    opening_date DATE,
    manager_name TEXT,
    city TEXT,
    batch_date DATE NOT NULL
);

