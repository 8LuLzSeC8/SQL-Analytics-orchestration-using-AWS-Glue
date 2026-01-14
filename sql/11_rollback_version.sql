-- =============================================================================
-- Procedure: rollback_version
-- Purpose : Roll back an SCD2 record to a previous version
-- Inputs  :
--   p_location_id    - natural key for the record
--   p_target_version - version number to roll back to
--   p_rolled_back_by - user performing the rollback
--   p_reason         - reason for the rollback
-- Change Log:
--   2026-01-13: Initial version
-- =============================================================================

CREATE OR REPLACE PROCEDURE governed_mdm.rollback_version (
    IN p_location_id INTEGER,
    IN p_target_version INTEGER,
    IN p_rolled_back_by TEXT,
    IN p_reason TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_new_version INTEGER;
BEGIN
    -- Close current record
    UPDATE governed_mdm.zone_dim_scd2
    SET
        effective_to = now(),
        is_current = false
    WHERE location_id = p_location_id
      AND is_current = true;

    -- Get next version number
    SELECT COALESCE(MAX(version_number), 0) + 1
    INTO v_new_version
    FROM governed_mdm.zone_dim_scd2
    WHERE location_id = p_location_id;

    -- Insert rolled-back version
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
        created_by,
        created_at,
        change_reason,
        change_source
    )
    SELECT
        location_id,
        borough,
        zone,
        service_zone,
        v_new_version,
        now(),
        NULL,
        true,
        'ROLLED_BACK',
        p_rolled_back_by,
        now(),
        p_reason,
        'rollback_procedure'
    FROM governed_mdm.zone_dim_scd2
    WHERE location_id = p_location_id
      AND version_number = p_target_version;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Target version % not found for location_id %',
            p_target_version, p_location_id;
    END IF;
END;
$$;


-- =============================================================================
-- Example usage:
-- CALL governed_mdm.rollback_version(1, 2, 'admin_user', 'Reverting to version 2');
-- =============================================================================