-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Data Platform / Analytics Engineering
-- Purpose: Create governed schemas for transformations, data quality, and marts

-- Notes:
--   - Safe to run multiple times
-- Change Log:
--   2026-01-13: Initial version
-- =============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS core;
COMMENT ON SCHEMA core IS 'Curated/core transformation layer in PostgreSQL.';

CREATE SCHEMA IF NOT EXISTS dq;
COMMENT ON SCHEMA dq IS 'Data quality framework: tests, reusable DQ functions, and results logging.';

CREATE SCHEMA IF NOT EXISTS marts;
COMMENT ON SCHEMA marts IS 'Business-facing reporting layer (views/materialized views).';

COMMIT;
