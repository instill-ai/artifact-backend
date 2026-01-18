-- Down migration: Remove unique constraints for resource IDs
DROP INDEX IF EXISTS idx_knowledge_base_namespace_id;
DROP INDEX IF EXISTS idx_file_kb_id;
