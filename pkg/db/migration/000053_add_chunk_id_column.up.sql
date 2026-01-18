-- Migration: Add ID column to chunk table
-- Following AIP standard: resource ID must be unique within parent scope
--
-- Chunk resource pattern: namespaces/{namespace}/files/{file}/chunks/{chunk}
-- The chunk ID should be unique within the file scope
-- ============================================================================
-- Step 1: Add id column to chunk table
-- ============================================================================
ALTER TABLE chunk
ADD COLUMN IF NOT EXISTS id VARCHAR(255);
-- ============================================================================
-- Step 2: Backfill existing chunks with hash-based IDs
-- Format: chk-{12-char-hash} derived from UID
-- ============================================================================
UPDATE chunk
SET id = 'chk-' || SUBSTRING(REPLACE(uid::text, '-', ''), 1, 12)
WHERE id IS NULL;
-- ============================================================================
-- Step 3: Add NOT NULL constraint after backfill
-- ============================================================================
ALTER TABLE chunk
ALTER COLUMN id
SET NOT NULL;
-- ============================================================================
-- Step 4: Create unique index on (file_uid, id)
-- ============================================================================
CREATE UNIQUE INDEX IF NOT EXISTS idx_chunk_file_id ON chunk(file_uid, id);
-- ============================================================================
-- Documentation
-- ============================================================================
COMMENT ON COLUMN chunk.id IS 'AIP standard: Immutable canonical ID with prefix (e.g., chk-8f3A2k9E7c1)';
COMMENT ON INDEX idx_chunk_file_id IS 'AIP standard: chunk ID unique within file scope';
