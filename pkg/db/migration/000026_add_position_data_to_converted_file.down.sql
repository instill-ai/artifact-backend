BEGIN;

ALTER TABLE text_chunk DROP COLUMN IF EXISTS reference;
ALTER TABLE converted_file DROP COLUMN IF EXISTS position_data;

COMMIT;
