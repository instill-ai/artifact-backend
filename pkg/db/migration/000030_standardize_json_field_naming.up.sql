BEGIN;
-- Standardize JSON field names to use PascalCase (matching Go struct field names)
-- This ensures consistency between old and new data formats
-- Fix text_chunk.reference column
-- Old format: {"page_range": [1, 1]}
-- New format: {"PageRange": [1, 1]}
UPDATE text_chunk
SET reference = jsonb_set(
        reference - 'page_range',
        '{PageRange}',
        reference->'page_range'
    )
WHERE reference ? 'page_range';
-- Fix converted_file.position_data column
-- Old format: {"page_delimiters": [100, 200]}
-- New format: {"PageDelimiters": [100, 200]}
UPDATE converted_file
SET position_data = jsonb_set(
        position_data - 'page_delimiters',
        '{PageDelimiters}',
        position_data->'page_delimiters'
    )
WHERE position_data ? 'page_delimiters';
-- Add comments explaining the format
COMMENT ON COLUMN text_chunk.reference IS 'Position data for visual grounding (JSON fields use PascalCase matching Go struct fields, e.g., PageRange)';
COMMENT ON COLUMN converted_file.position_data IS 'Position data for visual grounding (JSON fields use PascalCase matching Go struct fields, e.g., PageDelimiters)';
COMMIT;
