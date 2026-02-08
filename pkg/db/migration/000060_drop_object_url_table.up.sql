-- Drop the object_url table.
-- This table stored presigned MinIO upload/download URLs with expiration times.
-- It is no longer used: presigned URLs are now generated on-demand without
-- being persisted, and the GetObjectURLAdmin RPC has been deprecated.
-- All existing rows were expired (url_expire_at < NOW()) with no active
-- code paths inserting, reading, or deleting from this table.

-- First drop any indexes on the table
DROP INDEX IF EXISTS idx_object_url_object_uid;
DROP INDEX IF EXISTS idx_object_url_namespace_uid_object_uid;

-- Drop the table (CASCADE handles foreign key from object_url -> object)
DROP TABLE IF EXISTS object_url;
