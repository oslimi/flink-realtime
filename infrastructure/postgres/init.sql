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

