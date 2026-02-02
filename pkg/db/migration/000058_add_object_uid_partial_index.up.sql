-- ============================================================================
-- Migration: Add partial index on object(uid) for soft-delete queries
-- ============================================================================
-- This migration adds a partial index to optimize queries like:
--   SELECT ... FROM object WHERE uid = '...' AND delete_time IS NULL
--
-- The primary key on uid doesn't include delete_time, so the query planner
-- must perform a filter step after the index scan. Under high concurrency
-- (e.g., many simultaneous file uploads), this causes slow queries (200-400ms).
--
-- A partial index allows direct lookups without filtering.
-- ============================================================================
-- Note: Not using CONCURRENTLY as it cannot run in a transaction,
-- and most migration frameworks require transaction support.
CREATE INDEX IF NOT EXISTS idx_object_uid_not_deleted ON object(uid)
WHERE delete_time IS NULL;
COMMENT ON INDEX idx_object_uid_not_deleted IS 'Partial index for efficient lookup of non-deleted objects by uid';
