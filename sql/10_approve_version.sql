-- =============================================================================
-- Procedure: approve_version
-- Purpose : Approve current version of an SCD2 record
-- Inputs  :
--   p_zone_dim_sk   - surrogate key of the record to approve
--   p_approved_by   - user approving the record   
--   p_reason        - reason for approval
-- Change Log:
--   2026-01-13: Initial version
-- =============================================================================

CREATE OR REPLACE PROCEDURE governed_mdm.approve_version (
    IN p_zone_dim_sk BIGINT,
    IN p_approved_by TEXT,
    IN p_reason TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE governed_mdm.zone_dim_scd2
    SET
        record_status = 'APPROVED',
        approved_by   = p_approved_by,
        approved_at   = now(),
        change_reason = p_reason
    WHERE zone_dim_sk = p_zone_dim_sk
      AND is_current = true;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'No current record found for zone_dim_sk=%', p_zone_dim_sk;
    END IF;
END;
$$;

-- =============================================================================
-- Example usage:
-- CALL governed_mdm.approve_version(1, 'admin_user', 'Initial approval');
-- =============================================================================