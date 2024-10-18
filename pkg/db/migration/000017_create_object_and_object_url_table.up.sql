BEGIN;
-- Create object table
CREATE TABLE IF NOT EXISTS object (
    uid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(1040),
    size BIGINT ,
    content_type VARCHAR(255),
    namespace_uid UUID NOT NULL,
    creator_uid UUID NOT NULL,
    is_uploaded BOOLEAN NOT NULL DEFAULT FALSE,
    destination VARCHAR(255),
    object_expire_days INTEGER,
    last_modified_time TIMESTAMP,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    delete_time TIMESTAMP
);
-- Create indexes
CREATE INDEX IF NOT EXISTS idx_object_namespace_uid_creator ON object(namespace_uid, creator_uid);
-- Add comments for all columns
COMMENT ON COLUMN object.uid IS 'Unique identifier(uuid) for the object';
COMMENT ON COLUMN object.name IS 'Name of the object';
COMMENT ON COLUMN object.size IS 'Size of the object in bytes';
COMMENT ON COLUMN object.content_type IS 'MIME type of the object';
COMMENT ON COLUMN object.namespace_uid IS 'Namespace identifier(uuid) for the object';
COMMENT ON COLUMN object.creator_uid IS 'Creator of the object';
COMMENT ON COLUMN object.is_uploaded IS 'Flag indicating if the object is uploaded';
COMMENT ON COLUMN object.destination IS 'The destination of the object in the object storage';
COMMENT ON COLUMN object.object_expire_days IS 'The number of days the object will be expired';
COMMENT ON COLUMN object.last_modified_time IS 'Timestamp when the local file was last modified';
COMMENT ON COLUMN object.create_time IS 'Timestamp when the object was created';
COMMENT ON COLUMN object.update_time IS 'Timestamp when the object was last updated';
COMMENT ON COLUMN object.delete_time IS 'Timestamp when the object was deleted';
-- Create object_url table
CREATE TABLE IF NOT EXISTS object_url (
    uid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace_uid UUID NOT NULL,
    object_uid UUID NOT NULL REFERENCES object(uid) ON DELETE CASCADE,
    url_expire_at TIMESTAMP ,
    minio_url_path TEXT NOT NULL,
    encoded_url_path TEXT NOT NULL,
    type VARCHAR(10) NOT NULL CHECK (type IN ('upload', 'download')),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    delete_time TIMESTAMP
);
-- Add comments for all columns
COMMENT ON COLUMN object_url.uid IS 'Unique identifier(uuid) for the object url';
COMMENT ON COLUMN object_url.namespace_uid IS 'Namespace identifier(uuid) for the object url';
COMMENT ON COLUMN object_url.object_uid IS 'Object identifier(uuid) for the object url';
COMMENT ON COLUMN object_url.url_expire_at IS 'Timestamp when the object url will be expired';
COMMENT ON COLUMN object_url.minio_url_path IS 'The minio url path for the object';
COMMENT ON COLUMN object_url.encoded_url_path IS 'The encoded url path for the object';
COMMENT ON COLUMN object_url.type IS 'The type of the object url';
COMMENT ON COLUMN object_url.create_time IS 'Timestamp when the object url was created';
COMMENT ON COLUMN object_url.update_time IS 'Timestamp when the object url was last updated';
COMMENT ON COLUMN object_url.delete_time IS 'Timestamp when the object url was deleted';
-- Create indexes
CREATE INDEX IF NOT EXISTS idx_namespace_uid_object_uid ON object_url(namespace_uid, object_uid);
COMMIT;
