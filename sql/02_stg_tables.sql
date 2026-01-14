--==============================================================================
-- Author: Data Engineering Team
-- Owner: Analytics Engineering
-- Purpose: Staging table for enriched taxi trips data (with MDM zones)
-- Dependencies: None
-- Change Log:
--   - 2024-06-10: Initial version
--==============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.stg_trips_enriched (
  vendorid               integer,
  tpep_pickup_datetime   timestamp,
  tpep_dropoff_datetime  timestamp,
  passenger_count        bigint,
  trip_distance          double precision,
  ratecodeid             bigint,
  store_and_fwd_flag     text,
  pulocationid           integer,
  dolocationid           integer,
  payment_type           bigint,
  fare_amount            double precision,
  extra                  double precision,
  mta_tax                double precision,
  tip_amount             double precision,
  tolls_amount           double precision,
  improvement_surcharge  double precision,
  total_amount           double precision,
  congestion_surcharge   double precision,
  airport_fee            double precision,
  cbd_congestion_fee     double precision,
  pu_borough             text,
  pu_zone                text,
  pu_servicezone         text,
  do_borough             text,
  do_zone                text,
  do_servicezone         text,
  loaded_at              timestamptz NOT NULL DEFAULT now()
);

COMMIT;
