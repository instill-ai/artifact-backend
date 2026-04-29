DROP INDEX IF EXISTS idx_file_kb_unique_content_sha256;
ALTER TABLE file_knowledge_base DROP COLUMN IF EXISTS content_sha256;
