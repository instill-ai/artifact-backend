BEGIN;
-- Create the conversation table
CREATE TABLE conversation (
    uid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace_uid UUID NOT NULL,
    catalog_uid UUID NOT NULL,
    id VARCHAR(255) NOT NULL,
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delete_time TIMESTAMP
);
-- Create unique index on namespace_uid, catalog_uid, and id
CREATE UNIQUE INDEX idx_unique_namespace_catalog_id ON conversation (namespace_uid, catalog_uid, id)
WHERE delete_time IS NULL;
-- Create the message table
CREATE TABLE message (
    uid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace_uid UUID NOT NULL,
    catalog_uid UUID NOT NULL,
    conversation_uid UUID NOT NULL,
    content TEXT,
    role VARCHAR(50) NOT NULL,
    type VARCHAR(50) NOT NULL,
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delete_time TIMESTAMP
);
-- Add foreign key constraint with CASCADE DELETE
ALTER TABLE message
ADD CONSTRAINT fk_message_conversation FOREIGN KEY (conversation_uid) REFERENCES conversation(uid) ON DELETE CASCADE;
-- Add index for efficient message retrieval
CREATE INDEX idx_message_catalog_conversation ON message (namespace_uid, catalog_uid, conversation_uid);
-- Add comments
COMMENT ON TABLE conversation IS 'Table to store conversations';
COMMENT ON COLUMN conversation.uid IS 'Unique identifier(uuid) for the conversation';
COMMENT ON COLUMN conversation.namespace_uid IS 'Namespace identifier(uuid) for the conversation';
COMMENT ON COLUMN conversation.catalog_uid IS 'Catalog identifier(uuid) for the conversation';
COMMENT ON COLUMN conversation.id IS 'User-defined identifier for the conversation';
COMMENT ON COLUMN conversation.create_time IS 'Timestamp when the conversation was created';
COMMENT ON COLUMN conversation.update_time IS 'Timestamp when the conversation was last updated';
COMMENT ON COLUMN conversation.delete_time IS 'Timestamp when the conversation was deleted (soft delete)';
COMMENT ON TABLE message IS 'Table to store messages within conversations';
COMMENT ON COLUMN message.uid IS 'Unique identifier(uuid) for the message';
COMMENT ON COLUMN message.namespace_uid IS 'Namespace identifier(uuid) for the message';
COMMENT ON COLUMN message.catalog_uid IS 'Catalog identifier(uuid) for the message';
COMMENT ON COLUMN message.conversation_uid IS 'Reference to the conversation this message belongs to';
COMMENT ON COLUMN message.content IS 'Content of the message';
COMMENT ON COLUMN message.role IS 'Role of the message sender (e.g., user, assistant)';
COMMENT ON COLUMN message.type IS 'Type of the message';
COMMENT ON COLUMN message.create_time IS 'Timestamp when the message was created';
COMMENT ON COLUMN message.update_time IS 'Timestamp when the message was last updated';
COMMENT ON COLUMN message.delete_time IS 'Timestamp when the message was deleted (soft delete)';
COMMIT;
