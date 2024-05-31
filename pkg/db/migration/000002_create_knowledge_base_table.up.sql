CREATE TABLE knowledge_base (
    id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    kb_id VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    description VARCHAR(1023),
    tags VARCHAR(255)[],
    owner VARCHAR(255) NOT NULL,
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delete_time TIMESTAMP,
    CONSTRAINT unique_owner_name UNIQUE (owner, name)
);

COMMENT ON TABLE knowledge_base IS 'Table to store knowledge base information';
COMMENT ON COLUMN knowledge_base.id IS 'Primary key, auto-generated UUID';
COMMENT ON COLUMN knowledge_base.name IS 'Name of the knowledge base, up to 255 characters';
COMMENT ON COLUMN knowledge_base.kb_id IS 'Unique identifier for the knowledge base, up to 255 characters';
COMMENT ON COLUMN knowledge_base.description IS 'Description of the knowledge base, up to 1023 characters';
COMMENT ON COLUMN knowledge_base.update_time IS 'Timestamp of the last update, stored in UTC';
COMMENT ON COLUMN knowledge_base.create_time IS 'Timestamp when the entry was created, stored in UTC';
COMMENT ON COLUMN knowledge_base.owner IS 'Owner of the knowledge base, up to 255 characters';
COMMENT ON COLUMN knowledge_base.tags IS 'Array of tags associated with the knowledge base';
COMMENT ON COLUMN knowledge_base.delete_time IS 'Timestamp when the entry was marked as deleted, stored in UTC';