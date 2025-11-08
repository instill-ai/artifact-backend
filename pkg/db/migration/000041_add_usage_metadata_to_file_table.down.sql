BEGIN;

-- Remove usage_metadata column
ALTER TABLE file
    DROP COLUMN IF EXISTS usage_metadata;

COMMIT;
