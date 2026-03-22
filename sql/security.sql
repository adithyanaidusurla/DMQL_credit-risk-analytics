-- =========================
-- SECURITY SETUP (RBAC)
-- =========================

-- Drop roles if re-running (optional for dev)
DROP ROLE IF EXISTS analyst;
DROP ROLE IF EXISTS app_user;

-- =========================
-- ROLE: ANALYST (READ-ONLY)
-- =========================
CREATE ROLE analyst;

-- Allow connection
GRANT CONNECT ON DATABASE postgres TO analyst;

-- Allow schema usage
GRANT USAGE ON SCHEMA public TO analyst;

-- Read-only access to all tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst;

-- Ensure future tables are also accessible
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO analyst;


-- =========================
-- ROLE: APP USER (READ/WRITE)
-- =========================
CREATE ROLE app_user;

-- Allow connection
GRANT CONNECT ON DATABASE postgres TO app_user;

-- Allow schema usage
GRANT USAGE ON SCHEMA public TO app_user;

-- Read + write access
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO app_user;

-- Allow usage of sequences (important for SERIAL columns)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app_user;

-- Ensure future tables/sequences inherit permissions
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE ON TABLES TO app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT USAGE, SELECT ON SEQUENCES TO app_user;
