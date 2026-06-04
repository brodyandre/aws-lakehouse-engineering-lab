CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_key BIGINT PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    customer_name VARCHAR(255),
    email VARCHAR(255),
    city VARCHAR(128),
    state VARCHAR(64),
    country VARCHAR(128),
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_key BIGINT PRIMARY KEY,
    product_id VARCHAR(64) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(128),
    unit_price DECIMAL(18, 2)
);

CREATE TABLE IF NOT EXISTS gold.dim_campaign (
    campaign_key BIGINT PRIMARY KEY,
    campaign_id VARCHAR(64) NOT NULL,
    campaign_name VARCHAR(255),
    channel VARCHAR(64),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(18, 2)
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(32) NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.fct_sales (
    sales_key BIGINT PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    order_item_id VARCHAR(64) NOT NULL,
    customer_key BIGINT NOT NULL,
    product_key BIGINT NOT NULL,
    campaign_key BIGINT,
    date_key INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    gross_amount DECIMAL(18, 2) NOT NULL,
    discount_amount DECIMAL(18, 2) NOT NULL,
    net_amount DECIMAL(18, 2) NOT NULL,
    payment_method VARCHAR(64),
    order_status VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS gold.fct_web_events (
    event_key BIGINT PRIMARY KEY,
    customer_key BIGINT NOT NULL,
    campaign_key BIGINT,
    date_key INTEGER NOT NULL,
    event_type VARCHAR(64),
    page VARCHAR(255),
    device VARCHAR(64),
    session_id VARCHAR(128)
);
