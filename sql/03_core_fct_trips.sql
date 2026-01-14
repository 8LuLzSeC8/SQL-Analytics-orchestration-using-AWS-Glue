-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Analytics Engineering
-- Purpose: Build analytics-ready taxi trips fact table enriched with MDM
-- Dependencies:
--   - stg.stg_trips_enriched
--   - mdm.taxi_zone_master
--   - mdm.taxi_rate_master
-- Quality expectations:
--   - pickup_ts and dropoff_ts not null
--   - dropoff_ts >= pickup_ts
--   - trip_distance >= 0
--   - total_amount not null
--   - Zone & ratecode enrichment measured via DQ tests (no fallback)
-- Grain: 1 row per trip
-- Refresh: Full rebuild (TRUNCATE + INSERT)
-- Change Log:
--   - 2024-06-10: Initial version
-- =============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.fct_trips (
  vendor_id              integer,
  pickup_ts              timestamp,
  dropoff_ts             timestamp,
  trip_duration_seconds  integer,

  passenger_count        bigint,
  trip_distance          double precision,

  ratecode_id            integer,
  ratecode_name          text,

  store_and_fwd_flag     text,

  pu_location_id         integer,
  pu_borough             text,
  pu_zone                text,
  pu_service_zone        text,

  do_location_id         integer,
  do_borough             text,
  do_zone                text,
  do_service_zone        text,

  payment_type           bigint,

  fare_amount            double precision,
  extra                  double precision,
  mta_tax                double precision,
  tip_amount             double precision,
  tolls_amount           double precision,
  improvement_surcharge  double precision,
  congestion_surcharge   double precision,
  airport_fee            double precision,
  cbd_congestion_fee     double precision,
  total_amount           double precision,

  loaded_at              timestamptz NOT NULL DEFAULT now()
);

TRUNCATE TABLE core.fct_trips;

WITH
-- 1) Source
src AS (
  SELECT
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount
  FROM stg.stg_trips_enriched
),

-- 2) Clean + standardize
clean AS (
  SELECT
    vendorid::integer         AS vendor_id,
    tpep_pickup_datetime      AS pickup_ts,
    tpep_dropoff_datetime     AS dropoff_ts,
    EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))::integer
                              AS trip_duration_seconds,

    passenger_count,
    CASE WHEN trip_distance < 0 THEN NULL ELSE trip_distance END AS trip_distance,

    ratecodeid::integer       AS ratecode_id,
    store_and_fwd_flag,
    pulocationid::integer     AS pu_location_id,
    dolocationid::integer     AS do_location_id,
    payment_type,

    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount
  FROM src
),

-- 3) Latest ratecode (versioned MDM)
rate_mdm AS (
  SELECT ratecode_id, ratecode_name
  FROM (
    SELECT
      ratecode_id,
      ratecode_name,
      version,
      ROW_NUMBER() OVER (PARTITION BY ratecode_id ORDER BY version DESC) AS rn
    FROM mdm.taxi_rate_code_master
  ) r
  WHERE rn = 1
),

-- 4) Current zone records (SCD MDM)
zone_current AS (
  SELECT location_id, borough, zone, service_zone
  FROM (
    SELECT
      location_id,
      borough,
      zone,
      service_zone,
      record_status,
      version,
      effective_from,
      effective_to,
      ROW_NUMBER() OVER (
        PARTITION BY location_id
        ORDER BY
          CASE WHEN record_status ILIKE 'ACTIVE' THEN 0 ELSE 1 END,
          version DESC
      ) AS rn
    FROM mdm.taxi_zone_master
    WHERE effective_from <= now()
      AND (effective_to IS NULL OR now() < effective_to)
  ) z
  WHERE rn = 1
),

-- 5) Enrich strictly from MDM
enriched AS (
  SELECT
    c.*,
    r.ratecode_name,

    pu.borough      AS pu_borough,
    pu.zone         AS pu_zone,
    pu.service_zone AS pu_service_zone,

    doo.borough      AS do_borough,
    doo.zone         AS do_zone,
    doo.service_zone AS do_service_zone
  FROM clean c
  LEFT JOIN rate_mdm r
    ON c.ratecode_id = r.ratecode_id
  LEFT JOIN zone_current pu
    ON c.pu_location_id = pu.location_id
  LEFT JOIN zone_current doo
    ON c.do_location_id = doo.location_id
)

INSERT INTO core.fct_trips (
  vendor_id, pickup_ts, dropoff_ts, trip_duration_seconds,
  passenger_count, trip_distance,
  ratecode_id, ratecode_name,
  store_and_fwd_flag,
  pu_location_id, pu_borough, pu_zone, pu_service_zone,
  do_location_id, do_borough, do_zone, do_service_zone,
  payment_type,
  fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
  congestion_surcharge, airport_fee, cbd_congestion_fee, total_amount
)
SELECT
  vendor_id, pickup_ts, dropoff_ts, trip_duration_seconds,
  passenger_count, trip_distance,
  ratecode_id, ratecode_name,
  store_and_fwd_flag,
  pu_location_id, pu_borough, pu_zone, pu_service_zone,
  do_location_id, do_borough, do_zone, do_service_zone,
  payment_type,
  fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
  congestion_surcharge, airport_fee, cbd_congestion_fee, total_amount
FROM enriched
WHERE
  pickup_ts IS NOT NULL
  AND dropoff_ts IS NOT NULL
  AND dropoff_ts >= pickup_ts
  AND total_amount IS NOT NULL
  AND (trip_distance IS NULL OR trip_distance >= 0);

COMMIT;
