-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Data Governance / MDM
-- Purpose: SCD Type 2 UPSERT (procedure) for governed_mdm.zone_dim_scd2
-- Notes:
--   - Creates a new version only when attributes change
--   - Expires previous current record
--   - Inserts new record as DRAFT
-- Outputs:
--   - OUT parameters: o_new_zone_dim_sk, o_new_version_number
-- Change Log:
--   - 2024-06-10: Initial version
-- =============================================================================

CREATE OR REPLACE PROCEDURE governed_mdm.upsert_zone_scd2(
  IN  p_location_id     integer,
  IN  p_borough         text,
  IN  p_zone            text,
  IN  p_service_zone    text,
  IN  p_created_by      text,
  IN  p_change_reason   text,
  IN  p_change_source   text,
  OUT o_new_zone_dim_sk bigint,
  OUT o_new_version_number integer
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_current governed_mdm.zone_dim_scd2%ROWTYPE;
  v_new_version integer;
BEGIN
  -- Lock current row to avoid concurrent updates
  SELECT *
  INTO v_current
  FROM governed_mdm.zone_dim_scd2
  WHERE location_id = p_location_id
    AND is_current = true
  FOR UPDATE;

  -- If no row exists yet: insert as version 1 (DRAFT)
  IF NOT FOUND THEN
    INSERT INTO governed_mdm.zone_dim_scd2 (
      location_id, borough, zone, service_zone,
      version_number, effective_from, effective_to, is_current,
      record_status, change_reason, change_source,
      created_by
    )
    VALUES (
      p_location_id, p_borough, p_zone, p_service_zone,
      1, now(), NULL, true,
      'DRAFT', p_change_reason, p_change_source,
      COALESCE(p_created_by, current_user)
    )
    RETURNING zone_dim_sk, version_number
    INTO o_new_zone_dim_sk, o_new_version_number;

    RETURN;
  END IF;

  -- If unchanged: return current identifiers (no new version created)
  IF COALESCE(v_current.borough,'') = COALESCE(p_borough,'')
     AND COALESCE(v_current.zone,'') = COALESCE(p_zone,'')
     AND COALESCE(v_current.service_zone,'') = COALESCE(p_service_zone,'')
  THEN
    o_new_zone_dim_sk := v_current.zone_dim_sk;
    o_new_version_number := v_current.version_number;
    RETURN;
  END IF;

  -- New version = current + 1
  v_new_version := v_current.version_number + 1;

  -- Expire current record
  UPDATE governed_mdm.zone_dim_scd2
  SET effective_to = now(),
      is_current = false
  WHERE zone_dim_sk = v_current.zone_dim_sk;

  -- Insert new record as DRAFT
  INSERT INTO governed_mdm.zone_dim_scd2 (
    location_id, borough, zone, service_zone,
    version_number, effective_from, effective_to, is_current,
    record_status, change_reason, change_source,
    created_by
  )
  VALUES (
    p_location_id, p_borough, p_zone, p_service_zone,
    v_new_version, now(), NULL, true,
    'DRAFT', p_change_reason, p_change_source,
    COALESCE(p_created_by, current_user)
  )
  RETURNING zone_dim_sk, version_number
  INTO o_new_zone_dim_sk, o_new_version_number;

END;
$$;


--==============================================================================
-- Example call:
-- CALL governed_mdm.upsert_zone_scd2(
--  1,
--  'Manhattan'::text,
--  'Zone Demo'::text,
--  'Boro Zone'::text,
--  'analyst_user'::text,
--  'demo change'::text,
--  'manual_test'::text,
--  NULL,
--  NULL
--  );
--==============================================================================