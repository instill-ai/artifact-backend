-- Migration: Make active_collection_uid NOT NULL and UNIQUE
-- Purpose: Ensure every KB has a unique collection UID that is different from KB UID
-- This prevents confusion between KB identity and collection identity
-- Step 1: Generate unique UUIDs for any existing KBs where active_collection_uid = uid
-- This ensures we don't have duplicates before adding unique constraint
UPDATE knowledge_base
SET active_collection_uid = gen_random_uuid()
WHERE active_collection_uid = uid
    AND staging = false;
-- Only update production KBs
-- Step 2: Ensure no NULL values exist (shouldn't happen with current code, but be safe)
UPDATE knowledge_base
SET active_collection_uid = gen_random_uuid()
WHERE active_collection_uid IS NULL;
-- Step 3: Add NOT NULL constraint
ALTER TABLE knowledge_base
ALTER COLUMN active_collection_uid
SET NOT NULL;
-- Step 4: Add UNIQUE constraint to prevent collection UID collisions
-- Note: Only unique for non-deleted KBs (partial index)
CREATE UNIQUE INDEX idx_kb_active_collection_uid_unique ON knowledge_base(active_collection_uid)
WHERE delete_time IS NULL;
-- Add helpful comment
COMMENT ON COLUMN knowledge_base.active_collection_uid IS 'Unique UUID of the Milvus collection used by this KB. Always different from KB UID to avoid confusion between KB identity and collection identity.';
