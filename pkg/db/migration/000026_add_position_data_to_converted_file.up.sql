BEGIN;

-- Add position_data column to converted_file table after destination column
ALTER TABLE converted_file ADD COLUMN IF NOT EXISTS position_data JSONB;

-- Add comment for the new column
COMMENT ON COLUMN converted_file.position_data IS 'Position data for visual grounding and page-level citation. Content will depend on the file type.';

COMMIT;
