CREATE TABLE IF NOT EXISTS kb_entity (
    uid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kb_uid UUID NOT NULL REFERENCES knowledge_base(uid) ON DELETE CASCADE,
    name TEXT NOT NULL,
    entity_type TEXT,
    UNIQUE(kb_uid, name)
);

CREATE TABLE IF NOT EXISTS kb_entity_file (
    entity_uid UUID NOT NULL REFERENCES kb_entity(uid) ON DELETE CASCADE,
    file_uid UUID NOT NULL REFERENCES file(uid) ON DELETE CASCADE,
    PRIMARY KEY (entity_uid, file_uid)
);

CREATE INDEX IF NOT EXISTS idx_kb_entity_kb_uid ON kb_entity(kb_uid);
CREATE INDEX IF NOT EXISTS idx_kb_entity_file_file_uid ON kb_entity_file(file_uid);
