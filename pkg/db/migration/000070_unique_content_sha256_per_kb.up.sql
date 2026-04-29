-- Add content_sha256 to file_knowledge_base for per-KB content uniqueness.
-- The content_sha256 column is denormalized from file.content_sha256 to enable
-- a UNIQUE constraint scoped to (kb_uid, content_sha256). This prevents
-- concurrent CreateFile RPCs from racing past the application-level
-- GetFileByContentSHA256InKB check and inserting duplicate files.

ALTER TABLE file_knowledge_base
ADD COLUMN IF NOT EXISTS content_sha256 VARCHAR(64);

-- Backfill existing rows from the file table.
UPDATE file_knowledge_base fkb
SET content_sha256 = f.content_sha256
FROM file f
WHERE fkb.file_uid = f.uid
  AND f.content_sha256 IS NOT NULL
  AND f.content_sha256 != ''
  AND fkb.content_sha256 IS NULL;

-- Deduplicate: for each (kb_uid, content_sha256) group with duplicates,
-- keep the oldest entry (earliest created_at) and NULL out the SHA256 on
-- the rest so the unique index can be created. The NULLed entries remain
-- in the junction table (their files still exist) but are excluded from
-- the partial unique index.
UPDATE file_knowledge_base fkb
SET content_sha256 = NULL
WHERE content_sha256 IS NOT NULL
  AND content_sha256 != ''
  AND ctid NOT IN (
    SELECT DISTINCT ON (kb_uid, content_sha256) ctid
    FROM file_knowledge_base
    WHERE content_sha256 IS NOT NULL AND content_sha256 != ''
    ORDER BY kb_uid, content_sha256, created_at ASC
  );

-- Partial unique index: only non-null, non-empty SHA256 values are constrained.
-- Allows files without content hashes (e.g., object-only references before
-- content is downloaded) to coexist freely.
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_kb_unique_content_sha256
ON file_knowledge_base (kb_uid, content_sha256)
WHERE content_sha256 IS NOT NULL AND content_sha256 != '';
