BEGIN;
-- Drop the existing table if exists to avoid conflicts
DROP TABLE IF EXISTS knowledge_base;
-- Create the new knowledge_base table
CREATE TABLE knowledge_base (
    uid UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description VARCHAR(1023),
    tags VARCHAR(255) [],
    owner UUID NOT NULL,
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delete_time TIMESTAMP
);
-- Add the index
CREATE UNIQUE INDEX idx_unique_owner_name_delete_time ON knowledge_base (owner, name)
WHERE delete_time IS NULL;
CREATE UNIQUE INDEX unique_owner_id_delete_time ON knowledge_base (owner, id)
WHERE delete_time IS NULL;

-- Comments for the table and columns
COMMENT ON TABLE knowledge_base IS 'Table to store knowledge base information';
COMMENT ON COLUMN knowledge_base.uid IS 'Primary key, auto-generated UUID';
COMMENT ON COLUMN knowledge_base.id IS 'Unique identifier from name for the knowledge base, up to 255 characters created from name';
COMMENT ON COLUMN knowledge_base.name IS 'Name of the knowledge base, up to 255 characters';
COMMENT ON COLUMN knowledge_base.description IS 'Description of the knowledge base, up to 1023 characters';
COMMENT ON COLUMN knowledge_base.tags IS 'Array of tags associated with the knowledge base';
COMMENT ON COLUMN knowledge_base.owner IS 'Owner of the knowledge base. It is a UUID referencing the owner table(uid field).';
COMMENT ON COLUMN knowledge_base.create_time IS 'Timestamp when the entry was created, stored in UTC';
COMMENT ON COLUMN knowledge_base.update_time IS 'Timestamp of the last update, stored in UTC';
COMMENT ON COLUMN knowledge_base.delete_time IS 'Timestamp when the entry was marked as deleted, stored in UTC';
COMMIT;
