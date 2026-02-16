-- Migration: Enable pg_trgm for fuzzy search on knowledge base and file names.
--
-- pg_trgm provides trigram-based similarity matching which supports:
--   - Typo tolerance (e.g. "dte" matches "Date")
--   - Relevance ranking via similarity scores
--   - GIN index support for fast lookups
--
-- These indexes support the SearchEntities endpoint used by agent-backend
-- for the mention panel's KB and file search.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- GIN trigram indexes for fuzzy search on display_name
CREATE INDEX IF NOT EXISTS idx_knowledge_base_display_name_trgm
    ON knowledge_base USING gin (display_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_file_display_name_trgm
    ON file USING gin (display_name gin_trgm_ops);
