BEGIN;
-- Add unique constraint to prevent duplicate converted files for the same file and type
-- This prevents race conditions where concurrent processing creates multiple summaries
-- Step 1: Clean up existing duplicates, keeping only the most recent one
WITH duplicates AS (
    SELECT uid,
        ROW_NUMBER() OVER (
            PARTITION BY file_uid,
            converted_type
            ORDER BY create_time DESC
        ) as rn
    FROM converted_file
)
DELETE FROM converted_file
WHERE uid IN (
        SELECT uid
        FROM duplicates
        WHERE rn > 1
    );
-- Step 2: Drop the existing non-unique index
DROP INDEX IF EXISTS idx_converted_file_file_uid_converted_type;
-- Step 3: Create a unique index to enforce the constraint going forward
CREATE UNIQUE INDEX idx_unique_converted_file_file_uid_type ON converted_file (file_uid, converted_type);
COMMIT;
