-- Migration: Decouple File from KnowledgeBase
--
-- Files are now owned by Namespace (not KnowledgeBase).
-- A file can be associated with multiple knowledge bases via a junction table.
--
-- Changes:
-- 1. Create junction table for many-to-many file-kb relationship
-- 2. Migrate existing relationships to junction table
-- 3. Change unique constraint from (kb_uid, id) to (namespace_uid, id)
-- 4. Drop kb_uid column from file table
-- ============================================================================
-- Step 1: Create junction table for many-to-many relationship
-- ============================================================================
CREATE TABLE IF NOT EXISTS file_knowledge_base (
    file_uid UUID NOT NULL REFERENCES file(uid) ON DELETE CASCADE,
    kb_uid UUID NOT NULL REFERENCES knowledge_base(uid) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (file_uid, kb_uid)
);
CREATE INDEX IF NOT EXISTS idx_file_knowledge_base_file ON file_knowledge_base(file_uid);
CREATE INDEX IF NOT EXISTS idx_file_knowledge_base_kb ON file_knowledge_base(kb_uid);
COMMENT ON TABLE file_knowledge_base IS 'Junction table for many-to-many relationship between files and knowledge bases';
-- ============================================================================
-- Step 2: Migrate existing file-kb relationships to junction table
-- ============================================================================
INSERT INTO file_knowledge_base (file_uid, kb_uid, created_at)
SELECT uid,
    kb_uid,
    COALESCE(create_time, NOW())
FROM file
WHERE kb_uid IS NOT NULL ON CONFLICT (file_uid, kb_uid) DO NOTHING;
-- ============================================================================
-- Step 3: Drop old unique constraint (kb_uid, id)
-- ============================================================================
DROP INDEX IF EXISTS idx_file_kb_id;
-- ============================================================================
-- Step 4: Create new unique constraint on (namespace_uid, id)
-- File ID is now unique within namespace scope
-- ============================================================================
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_namespace_id ON file(namespace_uid, id)
WHERE delete_time IS NULL;
-- ============================================================================
-- Step 5: Drop kb_uid column from file table
-- ============================================================================
ALTER TABLE file DROP COLUMN IF EXISTS kb_uid;
-- ============================================================================
-- Documentation
-- ============================================================================
COMMENT ON INDEX idx_file_namespace_id IS 'AIP standard: file ID unique within namespace (soft-delete aware)';
