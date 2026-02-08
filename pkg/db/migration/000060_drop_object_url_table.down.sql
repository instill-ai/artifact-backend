-- Re-create the object_url table (restored from 000017_create_object_and_object_url_table.up.sql)
CREATE TABLE IF NOT EXISTS object_url (
    uid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace_uid UUID NOT NULL,
    object_uid UUID NOT NULL REFERENCES object(uid) ON DELETE CASCADE,
    url_expire_at TIMESTAMPTZ,
    minio_url_path TEXT NOT NULL,
    encoded_url_path TEXT NOT NULL,
    type VARCHAR(10) NOT NULL CHECK (type IN ('upload', 'download')),
    create_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    delete_time TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_object_url_namespace_uid_object_uid
    ON object_url(namespace_uid, object_uid);
