-- Mark video, audio, markdown, and text files for re-processing so they get
-- the new fine-grained chunking (timestamp-based for media, heading-based for
-- markdown/text) and entity extraction.
--
-- Setting status to WAITING_PROCESS triggers the existing ProcessFileWorkflow
-- which will re-chunk and re-summarize using the enhanced pipeline.
UPDATE file
SET process_status = 'FILE_PROCESS_STATUS_WAITING_PROCESS'
WHERE process_status = 'FILE_PROCESS_STATUS_COMPLETED'
  AND file_type IN (
    'TYPE_MP4', 'TYPE_WEBM', 'TYPE_MKV', 'TYPE_AVI', 'TYPE_MOV',
    'TYPE_MP3', 'TYPE_WAV', 'TYPE_OGG', 'TYPE_AAC', 'TYPE_FLAC', 'TYPE_M4A', 'TYPE_AIFF',
    'TYPE_MARKDOWN', 'TYPE_TEXT'
  );
