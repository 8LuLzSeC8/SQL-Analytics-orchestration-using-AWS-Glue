-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Data Governance / MDM
-- Purpose: SCD Type 2 dimension for taxi zones with governance metadata
-- Dependencies:
--   - (Optional seed) mdm.taxi_zone_master
-- Quality expectations:
--   - One and only one current record per location_id
--   - No overlapping effective windows per location_id (enforced by procedures)
-- Grain: 1 row per (location_id, version_number)
-- Refresh: Incremental via SCD procedures (no overwrite)
-- Change Log:
--   - 2024-06-10: Initial version
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS governed_mdm;

CREATE TABLE IF NOT EXISTS governed_mdm.zone_dim_scd2 (
  zone_dim_sk        bigserial PRIMARY KEY,          -- surrogate key
  location_id        integer NOT NULL,               -- natural key

  borough            text,
  zone               text,
  service_zone       text,

  version_number     integer NOT NULL,
  effective_from     timestamptz NOT NULL,
  effective_to       timestamptz,                    -- NULL = open-ended
  is_current         boolean NOT NULL DEFAULT true,

  record_status      text NOT NULL DEFAULT 'DRAFT',  -- DRAFT / APPROVED / REJECTED / ROLLED_BACK
  change_reason      text,
  change_source      text,

  created_at         timestamptz NOT NULL DEFAULT now(),
  created_by         text NOT NULL DEFAULT current_user,
  approved_at        timestamptz,
  approved_by        text,

  CONSTRAINT ck_zone_status CHECK (record_status IN ('DRAFT','APPROVED','REJECTED','ROLLED_BACK')),
  CONSTRAINT ck_effective_window CHECK (effective_to IS NULL OR effective_to > effective_from)
);

-- Ensure only one "current" record per location_id
CREATE UNIQUE INDEX IF NOT EXISTS ux_zone_dim_current
ON governed_mdm.zone_dim_scd2 (location_id)
WHERE is_current = true;

-- Point-in-time join performance
CREATE INDEX IF NOT EXISTS ix_zone_dim_time
ON governed_mdm.zone_dim_scd2 (location_id, effective_from, effective_to);

-- Version lookup performance
CREATE INDEX IF NOT EXISTS ix_zone_dim_version
ON governed_mdm.zone_dim_scd2 (location_id, version_number);
