CREATE INDEX IF NOT EXISTS idx_file_namespace_update_time_uid
    ON file(namespace_uid, update_time DESC, uid DESC)
    WHERE delete_time IS NULL;

CREATE INDEX IF NOT EXISTS idx_file_knowledge_base_kb_file
    ON file_knowledge_base(kb_uid, file_uid);
