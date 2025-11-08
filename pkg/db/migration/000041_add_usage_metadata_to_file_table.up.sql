BEGIN;
-- Add usage_metadata column to store AI usage metadata from content and summary generation
-- Format: {"content": {...}, "summary": {...}} where each contains the full UsageMetadata from AI response
ALTER TABLE file
ADD COLUMN usage_metadata JSONB DEFAULT NULL;
COMMENT ON COLUMN file.usage_metadata IS 'AI usage metadata from content and summary generation (VertexAI/Gemini)';
COMMIT;
