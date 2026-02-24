-- PostgreSQL Initialization Script
-- This script runs on first container startup

-- ===========================================
-- Application Database for Demos
-- ===========================================
CREATE DATABASE streaming_demo;

-- Create application user
CREATE USER app_user WITH ENCRYPTED PASSWORD 'app_password';

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE streaming_demo TO app_user;

-- Connect to streaming_demo and set up schema
\c streaming_demo

-- Grant schema privileges to app_user
GRANT ALL ON SCHEMA public TO app_user;

-- ===========================================
-- Fraud Detection Tables
-- ===========================================

-- Fraud Reports table
CREATE TABLE fraud_reports (
    id SERIAL PRIMARY KEY,
    report_id VARCHAR(100) UNIQUE NOT NULL,
    report_timestamp BIGINT NOT NULL,
    window_start BIGINT NOT NULL,
    window_end BIGINT NOT NULL,
    account_id VARCHAR(100) NOT NULL,
    total_alerts INTEGER NOT NULL,
    total_fraud_amount DECIMAL(15,2) NOT NULL,
    alert_ids TEXT,
    summary TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster queries
CREATE INDEX idx_fraud_reports_account_id ON fraud_reports(account_id);
CREATE INDEX idx_fraud_reports_timestamp ON fraud_reports(report_timestamp);

-- Grant privileges to app_user
GRANT ALL PRIVILEGES ON TABLE fraud_reports TO app_user;
GRANT USAGE, SELECT ON SEQUENCE fraud_reports_id_seq TO app_user;

-- ===========================================
-- Sample tables (uncomment when needed)
-- ===========================================
-- CREATE TABLE events (
--     id SERIAL PRIMARY KEY,
--     event_type VARCHAR(100),
--     payload JSONB,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- CREATE TABLE aggregations (
--     id SERIAL PRIMARY KEY,
--     key VARCHAR(255),
--     value NUMERIC,
--     window_start TIMESTAMP,
--     window_end TIMESTAMP,
--     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

