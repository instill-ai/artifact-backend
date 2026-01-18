BEGIN;

-- ============================================================================
-- Migration: Rename chunk.content_dest to storage_path
-- ============================================================================
-- This migration renames the confusing 'content_dest' column to 'storage_path'
-- for clarity and consistency with the object table.
-- ============================================================================

-- Rename the column
ALTER TABLE chunk RENAME COLUMN content_dest TO storage_path;

-- Update comment for documentation
COMMENT ON COLUMN chunk.storage_path IS 'Path to the chunk content in blob storage';

COMMIT;
