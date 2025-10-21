BEGIN;
-- Drop system table and its indexes (reverse order of creation)
DROP INDEX IF EXISTS idx_system_updated_at;
DROP TABLE IF EXISTS system;
-- Drop active_collection_uid from knowledge_base
DROP INDEX IF EXISTS idx_kb_active_collection_uid;
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS active_collection_uid;
-- Drop upgrade tracking fields from knowledge_base
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS rollback_retention_until;
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS update_completed_at;
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS update_started_at;
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS update_workflow_id;
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS update_status;
-- Drop staging boolean flag from knowledge_base
DROP INDEX IF EXISTS idx_kb_staging;
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS staging;
COMMIT;
