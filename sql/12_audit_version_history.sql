-- =============================================================================
-- Procedure: governed_mdm.audit_version_history
-- Purpose : Return version history for a single governed record (SCD2)
-- Inputs  :
--   p_location_id  - natural key for the record
--   p_from_date    - optional lower bound (inclusive) for effective_from
--   p_to_date      - optional upper bound (inclusive) for effective_from
-- Output  :
--   refcursor you can FETCH in the same transaction
-- Change Log:
--   2026-01-13: Initial version
-- =============================================================================

CREATE OR REPLACE PROCEDURE governed_mdm.audit_version_history (
    IN p_location_id INTEGER,
    IN p_from_date TIMESTAMPTZ DEFAULT NULL,
    IN p_to_date   TIMESTAMPTZ DEFAULT NULL,
    INOUT audit_cursor REFCURSOR DEFAULT 'audit_cur'
)
LANGUAGE plpgsql
AS $$
BEGIN
    OPEN audit_cursor FOR
        SELECT
            zone_dim_sk,
            location_id,
            borough,
            zone,
            service_zone,
            version_number,
            record_status,
            effective_from,
            effective_to,
            is_current,
            created_by,
            created_at,
            approved_by,
            approved_at,
            change_reason,
            change_source
        FROM governed_mdm.zone_dim_scd2
        WHERE location_id = p_location_id
          AND (p_from_date IS NULL OR effective_from >= p_from_date)
          AND (p_to_date   IS NULL OR effective_from <= p_to_date)
        ORDER BY version_number;
END;
$$;

-- =============================================================================
-- Example usage:
-- CALL governed_mdm.audit_version_history(1, NULL, NULL, 'audit');
-- FETCH ALL FROM audit;
-- =============================================================================
