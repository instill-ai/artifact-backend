ALTER TABLE file
ADD COLUMN content_sha256 VARCHAR(64);
CREATE INDEX idx_file_content_sha256 ON file (content_sha256)
WHERE delete_time IS NULL;
