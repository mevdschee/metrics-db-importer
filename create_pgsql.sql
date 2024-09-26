-- cat create_pgsql.sql | sudo -u postgres psql
--
DROP DATABASE "metrics";
DROP USER "metrics";
--
CREATE USER "metrics" WITH PASSWORD 'metrics';
CREATE DATABASE "metrics";
ALTER DATABASE "metrics" OWNER TO "metrics";
GRANT ALL PRIVILEGES ON DATABASE "metrics" to "metrics";
\c "metrics";
-- CREATE EXTENSION IF NOT EXISTS timescaledb;