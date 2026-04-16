-- Add visibility column to the file table so we can model who can discover
-- and access each file (mirrors collection.visibility). Default to
-- VISIBILITY_WORKSPACE so existing rows become workspace-visible, matching
-- today's implicit behaviour where org members can read every file in the
-- knowledge base.
ALTER TABLE file
    ADD COLUMN IF NOT EXISTS visibility VARCHAR(32) NOT NULL DEFAULT 'VISIBILITY_WORKSPACE';

-- Partial index for queries that filter by visibility (e.g. listing
-- link-shared files) without scanning soft-deleted rows.
CREATE INDEX IF NOT EXISTS idx_file_visibility
    ON file (visibility)
    WHERE delete_time IS NULL;
