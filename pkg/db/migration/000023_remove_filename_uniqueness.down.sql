CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_unique_kb_uid_name_delete_time
  ON knowledge_base_file (kb_uid, name)
  WHERE delete_time IS NULL;
