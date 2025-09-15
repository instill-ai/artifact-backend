BEGIN;

-- Add position_data column to converted_file table after destination column
ALTER TABLE converted_file ADD COLUMN IF NOT EXISTS position_data JSONB;
ALTER TABLE text_chunk ADD COLUMN IF NOT EXISTS reference JSONB;

-- Add comment for the new column
COMMENT ON COLUMN converted_file.position_data IS 'Position data for visual grounding and page-level citation. Content will depend on the file type.';
COMMENT ON COLUMN text_chunk.reference IS 'Position data for visual grounding and page-level citation. Content will depend on the file type.';

COMMIT;
