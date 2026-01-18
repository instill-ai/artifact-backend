-- Down migration: Restore File-KnowledgeBase 1:1 relationship
--
-- WARNING: This migration may lose data if files are associated with multiple KBs.
-- The first KB association will be kept in kb_uid.

-- Step 1: Add kb_uid column back
ALTER TABLE file ADD COLUMN IF NOT EXISTS kb_uid UUID;

-- Step 2: Restore kb_uid from junction table (take first association)
UPDATE file f
SET kb_uid = (
    SELECT fkb.kb_uid
    FROM file_knowledge_base fkb
    WHERE fkb.file_uid = f.uid
    ORDER BY fkb.created_at ASC
    LIMIT 1
);

-- Step 3: Make kb_uid NOT NULL
-- Note: This will fail if any files have no KB association
ALTER TABLE file ALTER COLUMN kb_uid SET NOT NULL;

-- Step 4: Add foreign key constraint
ALTER TABLE file ADD CONSTRAINT fk_file_kb
    FOREIGN KEY (kb_uid) REFERENCES knowledge_base(uid) ON DELETE CASCADE;

-- Step 5: Drop new unique constraint
DROP INDEX IF EXISTS idx_file_namespace_id;

-- Step 6: Restore old unique constraint
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_kb_id
    ON file(kb_uid, id)
    WHERE delete_time IS NULL;

-- Step 7: Drop junction table
DROP TABLE IF EXISTS file_knowledge_base;
