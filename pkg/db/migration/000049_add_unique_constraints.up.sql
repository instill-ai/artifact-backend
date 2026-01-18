-- Migration: Add unique constraints for resource IDs
-- Following AIP standard: resource ID must be unique within parent scope (soft-delete aware)
--
-- This migration adds missing unique constraints:
-- 1. knowledge_base: UNIQUE (namespace_uid, id) WHERE delete_time IS NULL
-- 2. file: UNIQUE (kb_uid, id) WHERE delete_time IS NULL
-- ============================================================================
-- Knowledge Base: Add unique constraint on (namespace_uid, id)
-- ============================================================================
CREATE UNIQUE INDEX IF NOT EXISTS idx_knowledge_base_namespace_id ON knowledge_base(namespace_uid, id)
WHERE delete_time IS NULL;
-- ============================================================================
-- File: Add unique constraint on (kb_uid, id)
-- ============================================================================
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_kb_id ON file(kb_uid, id)
WHERE delete_time IS NULL;
-- ============================================================================
-- Add comments for documentation
-- ============================================================================
COMMENT ON INDEX idx_knowledge_base_namespace_id IS 'AIP standard: knowledge base ID unique within namespace (soft-delete aware)';
COMMENT ON INDEX idx_file_kb_id IS 'AIP standard: file ID unique within knowledge base (soft-delete aware)';
