BEGIN;

-- Recreate the unique index for encoded_url_path
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_encoded_url_path ON object_url(encoded_url_path);

-- Add comment for the index
COMMENT ON INDEX idx_unique_encoded_url_path IS 'Ensures uniqueness of encoded URL paths across all object URLs';

COMMIT;
