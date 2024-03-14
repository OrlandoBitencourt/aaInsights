
SELECT 'user_logs'::regnamespace;

CREATE DATABASE IF NOT EXISTS user_logs;

-- Connect to the new database
\c user_logs;

-- Create a new role (user)
CREATE ROLE adm WITH LOGIN PASSWORD 'supersecret';
ALTER ROLE adm CREATEDB;


-- Create a table for users
CREATE TABLE IF NOT EXISTS users (
    user_hash TEXT PRIMARY KEY,
    user_name TEXT,
    faction TEXT
);

-- Create a table for logs
CREATE TABLE IF NOT EXISTS logs (
    log_type TEXT,
    time TEXT,
    character TEXT,
    receiver TEXT,
    total INTEGER,
    location TEXT,
    log_id TEXT PRIMARY KEY,
    character_id TEXT,
    receiver_id TEXT
);

-- Create indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_user_hash ON users (user_hash);
CREATE INDEX IF NOT EXISTS idx_logs_character_id ON logs (character_id);
CREATE INDEX IF NOT EXISTS idx_logs_receiver_id ON logs (receiver_id);
CREATE INDEX IF NOT EXISTS idx_logs_log_type ON logs (log_type);
CREATE INDEX IF NOT EXISTS idx_logs_time ON logs (time);
