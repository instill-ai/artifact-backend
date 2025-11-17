BEGIN;

-- Add parent_kb_uid column to establish parent-child relationship between production and staging/rollback KBs
-- This replaces the KBID suffix pattern ({kb-id}-staging, {kb-id}-rollback) with a proper FK relationship
-- Benefits:
-- 1. KBID remains purely user-controlled (no system manipulation)
-- 2. Clear parent-child relationship with database integrity
-- 3. Fast indexed lookups (no string manipulation or array scanning)
-- 4. Foreign key prevents orphaned staging/rollback KBs

ALTER TABLE knowledge_base
ADD COLUMN parent_kb_uid UUID REFERENCES knowledge_base(uid) ON DELETE CASCADE;

-- Create index for fast lookups of child KBs (staging/rollback) by parent KB
CREATE INDEX idx_kb_parent_kb_uid ON knowledge_base(parent_kb_uid);

COMMENT ON COLUMN knowledge_base.parent_kb_uid IS 'References the production KB UID for staging/rollback KBs. NULL for production KBs.';

-- For existing staging/rollback KBs, backfill parent_kb_uid by parsing KBID suffixes
-- Pattern 1: {production-kb-id}-staging → find parent by removing suffix
UPDATE knowledge_base child
SET parent_kb_uid = parent.uid
FROM knowledge_base parent
WHERE child.staging = true
  AND child.parent_kb_uid IS NULL
  AND (child.id LIKE '%-staging' OR child.id LIKE '%-rollback')
  AND child.owner = parent.owner
  AND parent.staging = false
  AND (
    -- Match staging: {parent-id}-staging
    (child.id LIKE '%-staging' AND parent.id = SUBSTRING(child.id FROM 1 FOR LENGTH(child.id) - 8))
    OR
    -- Match rollback: {parent-id}-rollback
    (child.id LIKE '%-rollback' AND parent.id = SUBSTRING(child.id FROM 1 FOR LENGTH(child.id) - 9))
  );

COMMIT;
