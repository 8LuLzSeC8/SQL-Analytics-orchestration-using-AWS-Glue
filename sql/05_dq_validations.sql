-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Analytics Engineering
-- Purpose: Data quality validation metrics for core.fct_trips (no pass/fail here)
-- Dependencies:
--   - core.fct_trips
-- Quality expectations:
--   - This query returns a single row of key DQ metrics for monitoring/testing
-- =============================================================================

WITH base AS (
  SELECT *
  FROM core.fct_trips
),
metrics AS (
  SELECT
    COUNT(*)::bigint AS total_rows,

    -- Completeness checks
    AVG((pickup_ts IS NOT NULL)::int)::numeric(10,6)  AS pickup_ts_fill_rate,
    AVG((dropoff_ts IS NOT NULL)::int)::numeric(10,6) AS dropoff_ts_fill_rate,
    AVG((total_amount IS NOT NULL)::int)::numeric(10,6) AS total_amount_fill_rate,

    -- Validity checks
    SUM((dropoff_ts < pickup_ts)::int)::bigint AS bad_time_order_count,
    SUM((trip_distance < 0)::int)::bigint      AS negative_distance_count,
    SUM((trip_duration_seconds < 0)::int)::bigint AS negative_duration_count,

    -- Enrichment completeness (MDM-only, no fallback)
    AVG((pu_zone IS NOT NULL)::int)::numeric(10,6) AS pu_zone_fill_rate,
    AVG((do_zone IS NOT NULL)::int)::numeric(10,6) AS do_zone_fill_rate,
    AVG((ratecode_name IS NOT NULL)::int)::numeric(10,6) AS ratecode_name_fill_rate

  FROM base
)
SELECT *
FROM metrics;
