BEGIN;
-- Add CHECK constraints to ensure all enum fields are always stored as enum strings, never as integers
-- This prevents bugs where protobuf enum integers might accidentally be stored instead of strings
-- All constraints use regex patterns to validate the format: PREFIX_UPPERCASE_WORDS
-- ============================================================================
-- TABLE: file
-- ============================================================================
-- FIRST: Update existing data to match the expected format
-- Remove "FILE_" prefix from file_type values (e.g., "FILE_TYPE_PDF" -> "TYPE_PDF")
UPDATE file
SET file_type = REGEXP_REPLACE(file_type, '^FILE_(TYPE_)', '\1')
WHERE file_type ~ '^FILE_TYPE_';
-- Constraint: file_type must start with "TYPE_" (e.g., "TYPE_PDF", "TYPE_TEXT")
ALTER TABLE file
ADD CONSTRAINT check_file_type_format CHECK (file_type ~ '^TYPE_[A-Z0-9_]+$');
COMMENT ON CONSTRAINT check_file_type_format ON file IS 'Ensures file_type is always stored as a string enum (e.g., TYPE_PDF), never as an integer';
-- Constraint: process_status must start with "FILE_PROCESS_STATUS_" (e.g., "FILE_PROCESS_STATUS_COMPLETED")
ALTER TABLE file
ADD CONSTRAINT check_process_status_format CHECK (process_status ~ '^FILE_PROCESS_STATUS_[A-Z_]+$');
COMMENT ON CONSTRAINT check_process_status_format ON file IS 'Ensures process_status is always stored as a string enum (e.g., FILE_PROCESS_STATUS_COMPLETED), never as an integer';
-- ============================================================================
-- TABLE: knowledge_base
-- ============================================================================
-- Constraint: update_status must start with "KNOWLEDGE_BASE_UPDATE_STATUS_" or be empty
-- Empty string is allowed as update_status is optional
ALTER TABLE knowledge_base
ADD CONSTRAINT check_update_status_format CHECK (
        update_status = ''
        OR update_status ~ '^KNOWLEDGE_BASE_UPDATE_STATUS_[A-Z_]+$'
    );
COMMENT ON CONSTRAINT check_update_status_format ON knowledge_base IS 'Ensures update_status is always stored as a string enum (e.g., KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED) or empty, never as an integer';
-- ============================================================================
-- TABLE: converted_file
-- ============================================================================
-- Constraint: converted_type must start with "CONVERTED_FILE_TYPE_" (e.g., "CONVERTED_FILE_TYPE_CONTENT", "CONVERTED_FILE_TYPE_SUMMARY")
ALTER TABLE converted_file
ADD CONSTRAINT check_converted_type_format CHECK (converted_type ~ '^CONVERTED_FILE_TYPE_[A-Z_]+$');
COMMENT ON CONSTRAINT check_converted_type_format ON converted_file IS 'Ensures converted_type is always stored as a string enum (e.g., CONVERTED_FILE_TYPE_CONTENT), never as an integer';
-- ============================================================================
-- TABLE: chunk
-- ============================================================================
-- Constraint: chunk_type must start with "TYPE_" (e.g., "TYPE_CONTENT", "TYPE_SUMMARY")
ALTER TABLE chunk
ADD CONSTRAINT check_chunk_type_format CHECK (chunk_type ~ '^TYPE_[A-Z_]+$');
COMMENT ON CONSTRAINT check_chunk_type_format ON chunk IS 'Ensures chunk_type is always stored as a string enum (e.g., TYPE_CONTENT), never as an integer';
-- ============================================================================
-- TABLE: embedding
-- ============================================================================
-- Constraint: chunk_type must start with "TYPE_" (e.g., "TYPE_CONTENT", "TYPE_SUMMARY")
ALTER TABLE embedding
ADD CONSTRAINT check_embedding_chunk_type_format CHECK (chunk_type ~ '^TYPE_[A-Z_]+$');
COMMENT ON CONSTRAINT check_embedding_chunk_type_format ON embedding IS 'Ensures chunk_type in embedding table is always stored as a string enum (e.g., TYPE_CONTENT), never as an integer';
COMMIT;
