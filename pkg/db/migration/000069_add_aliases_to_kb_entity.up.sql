-- Add bilingual / synonym alias surface forms to kb_entity so that ingestion
-- can persist the alternate names that are emitted by the summary LLM. This
-- powers cross-script retrieval (e.g. "chicken" -> "公雞") by landing the
-- aliases into the TYPE_AUGMENTED chunk that BM25 indexes.
--
-- Additive column; every existing row defaults to the empty array so no
-- back-fill of existing rows is required at migration time. A separate
-- Temporal activity + CLI subcommand populates aliases for already-ingested
-- files.
ALTER TABLE kb_entity
    ADD COLUMN IF NOT EXISTS aliases TEXT[] NOT NULL DEFAULT '{}'::TEXT[];

-- GIN index so future "any entity whose aliases contain X" queries can hit
-- an inverted index instead of scanning every row. Not required by the
-- current retrieval path (BM25 over the augmented chunk carries that load),
-- but future entity-hop / SQL rescue queries will want it.
CREATE INDEX IF NOT EXISTS idx_kb_entity_aliases
    ON kb_entity USING GIN (aliases);
