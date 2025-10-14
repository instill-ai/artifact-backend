BEGIN;
-- Revert JSON field names to snake_case for rollback compatibility
-- Revert text_chunk.reference column
-- New format: {"PageRange": [1, 1]}
-- Old format: {"page_range": [1, 1]}
UPDATE text_chunk
SET reference = jsonb_set(
        reference - 'PageRange',
        '{page_range}',
        reference->'PageRange'
    )
WHERE reference ? 'PageRange';
-- Revert converted_file.position_data column
-- New format: {"PageDelimiters": [100, 200]}
-- Old format: {"page_delimiters": [100, 200]}
UPDATE converted_file
SET position_data = jsonb_set(
        position_data - 'PageDelimiters',
        '{page_delimiters}',
        position_data->'PageDelimiters'
    )
WHERE position_data ? 'PageDelimiters';
-- Restore original comments
COMMENT ON COLUMN text_chunk.reference IS 'Position data for visual grounding and page-level citation. Content will depend on the file type.';
COMMENT ON COLUMN converted_file.position_data IS 'Position data for visual grounding and page-level citation. Content will depend on the file type.';
COMMIT;
