-- Migration: Add unique constraint on (namespace_uid, slug) for knowledge_base
-- Prevents duplicate default KBs caused by cross-replica race conditions in
-- GetOrCreateDefaultKnowledgeBase (singleflight only deduplicates within a single process).
CREATE UNIQUE INDEX IF NOT EXISTS idx_knowledge_base_namespace_slug
ON knowledge_base(namespace_uid, slug)
WHERE delete_time IS NULL;

COMMENT ON INDEX idx_knowledge_base_namespace_slug IS 'Prevent duplicate KBs with same slug per namespace (soft-delete aware)';
