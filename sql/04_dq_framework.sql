-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Analytics Engineering
-- Purpose: Data quality framework objects (schema, test results table, assert function)
-- Dependencies: None
-- Quality expectations:
--   - Every test run writes an auditable PASS/FAIL row
--   - Test results are append-only (do not update history)
-- Change Log:
--   - 2024-06-10: Initial version
-- =============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS dq;

-- Stores results of SQL-based tests (append-only audit)
CREATE TABLE IF NOT EXISTS dq.test_results (
  test_run_id   uuid            NOT NULL DEFAULT gen_random_uuid(),
  test_name     text            NOT NULL,
  passed        boolean         NOT NULL,
  metric_value  numeric         NULL,
  threshold     numeric         NULL,
  details       text            NULL,
  created_at    timestamptz     NOT NULL DEFAULT now(),
  created_by    text            NOT NULL DEFAULT current_user
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_test_results_created_at
  ON dq.test_results(created_at);

CREATE INDEX IF NOT EXISTS idx_test_results_test_name
  ON dq.test_results(test_name);

-- Reusable assertion function to record pass/fail
-- Note: requires pgcrypto extension for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION dq.assert_true(
  p_test_name text,
  p_passed boolean,
  p_metric_value numeric DEFAULT NULL,
  p_threshold numeric DEFAULT NULL,
  p_details text DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO dq.test_results (test_name, passed, metric_value, threshold, details)
  VALUES (p_test_name, p_passed, p_metric_value, p_threshold, p_details);
END;
$$;

COMMIT;
