-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Analytics Engineering
-- Purpose: SQL-based DQ tests for core.fct_trips (writes PASS/FAIL to dq.test_results)
-- Dependencies:
--   - core.fct_trips
--   - dq.assert_true()
--   - dq.test_results
-- Quality expectations:
--   - Each run writes one row per test into dq.test_results
-- Change Log:
--   - 2024-06-10: Initial version
-- =============================================================================

WITH m AS (
  SELECT
    COUNT(*)::bigint AS total_rows,

    AVG((pickup_ts IS NOT NULL)::int)::numeric(10,6)  AS pickup_ts_fill_rate,
    AVG((dropoff_ts IS NOT NULL)::int)::numeric(10,6) AS dropoff_ts_fill_rate,
    AVG((total_amount IS NOT NULL)::int)::numeric(10,6) AS total_amount_fill_rate,

    SUM((dropoff_ts < pickup_ts)::int)::bigint AS bad_time_order_count,
    SUM((trip_distance < 0)::int)::bigint      AS negative_distance_count,
    SUM((trip_duration_seconds < 0)::int)::bigint AS negative_duration_count,

    AVG((pu_zone IS NOT NULL)::int)::numeric(10,6) AS pu_zone_fill_rate,
    AVG((do_zone IS NOT NULL)::int)::numeric(10,6) AS do_zone_fill_rate,
    AVG((ratecode_name IS NOT NULL)::int)::numeric(10,6) AS ratecode_name_fill_rate
  FROM core.fct_trips
),

tests AS (
  SELECT
    dq.assert_true(
      'core.fct_trips has rows',
      (total_rows > 0),
      total_rows::numeric,
      1,
      'Expect at least 1 row in core.fct_trips'
    ) AS t1,

    dq.assert_true(
      'pickup_ts_fill_rate >= 1.00',
      (pickup_ts_fill_rate >= 1.00),
      pickup_ts_fill_rate,
      1.00,
      'Pickup timestamp should be present for all rows'
    ) AS t2,

    dq.assert_true(
      'dropoff_ts_fill_rate >= 1.00',
      (dropoff_ts_fill_rate >= 1.00),
      dropoff_ts_fill_rate,
      1.00,
      'Dropoff timestamp should be present for all rows'
    ) AS t3,

    dq.assert_true(
      'total_amount_fill_rate >= 1.00',
      (total_amount_fill_rate >= 1.00),
      total_amount_fill_rate,
      1.00,
      'Total amount should be present for all rows'
    ) AS t4,

    dq.assert_true(
      'bad_time_order_count = 0',
      (bad_time_order_count = 0),
      bad_time_order_count::numeric,
      0,
      'No rows should have dropoff_ts < pickup_ts'
    ) AS t5,

    dq.assert_true(
      'negative_distance_count = 0',
      (negative_distance_count = 0),
      negative_distance_count::numeric,
      0,
      'No rows should have negative trip_distance'
    ) AS t6,

    dq.assert_true(
      'negative_duration_count = 0',
      (negative_duration_count = 0),
      negative_duration_count::numeric,
      0,
      'No rows should have negative trip_duration_seconds'
    ) AS t7,

    dq.assert_true(
      'pu_zone_fill_rate >= 0.99',
      (pu_zone_fill_rate >= 0.99),
      pu_zone_fill_rate,
      0.99,
      'Pickup zone should be enriched for >= 99% rows'
    ) AS t8,

    dq.assert_true(
      'do_zone_fill_rate >= 0.99',
      (do_zone_fill_rate >= 0.99),
      do_zone_fill_rate,
      0.99,
      'Dropoff zone should be enriched for >= 99% rows'
    ) AS t9,

    dq.assert_true(
      'ratecode_name_fill_rate >= 0.70',
      (ratecode_name_fill_rate >= 0.70),
      ratecode_name_fill_rate,
      0.70,
      'Ratecode name enrichment is informational; require >= 70%'
    ) AS t10

  FROM m
)

-- CRITICAL: this forces Postgres to execute the tests CTE
SELECT COUNT(*) AS tests_executed
FROM tests;
