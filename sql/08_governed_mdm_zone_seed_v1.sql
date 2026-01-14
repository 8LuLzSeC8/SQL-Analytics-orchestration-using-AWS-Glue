-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Data Governance / MDM
-- Purpose: Seed initial (version 1) records into governed_mdm.zone_dim_scd2
-- Dependencies:
--   - mdm.taxi_zone_master
-- Quality expectations:
--   - One current row per location_id
--   - Seed uses most recent/current record from mdm if duplicates exist
-- Change Log:
--   2026-01-13: Initial version
-- =============================================================================

INSERT INTO governed_mdm.zone_dim_scd2 (
  location_id,
  borough,
  zone,
  service_zone,
  version_number,
  effective_from,
  effective_to,
  is_current,
  record_status,
  change_reason,
  change_source,
  created_by,
  approved_by,
  approved_at
)
SELECT
  z.location_id,
  z.borough,
  z.zone,
  z.service_zone,
  1 AS version_number,
  now() AS effective_from,
  NULL::timestamptz AS effective_to,
  true AS is_current,
  'APPROVED' AS record_status,
  'initial seed load' AS change_reason,
  'mdm.taxi_zone_master' AS change_source,
  'seed_load' AS created_by,
  'seed_load' AS approved_by,
  now() AS approved_at
FROM (
  -- If mdm has multiple rows per location_id, pick the best candidate
  SELECT
    location_id,
    borough,
    zone,
    service_zone,
    ROW_NUMBER() OVER (
      PARTITION BY location_id
      ORDER BY
        CASE WHEN record_status ILIKE 'ACTIVE' THEN 0 ELSE 1 END,
        version DESC NULLS LAST,
        updated_at DESC NULLS LAST,
        created_at DESC NULLS LAST
    ) AS rn
  FROM mdm.taxi_zone_master
) z
WHERE z.rn = 1
  -- prevent duplicates if script re-run
  AND NOT EXISTS (
    SELECT 1
    FROM governed_mdm.zone_dim_scd2 t
    WHERE t.location_id = z.location_id
      AND t.version_number = 1
  );
