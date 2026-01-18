-- Migration: Add slug column for AIP standard
-- Slug is a URL-friendly identifier without prefix, derived from display_name
-- Example: "my-knowledge-base" (not "kb-my-knowledge-base")

-- Add slug column to knowledge_base table
ALTER TABLE knowledge_base
ADD COLUMN IF NOT EXISTS slug VARCHAR(255);

-- Add slug column to file table
ALTER TABLE file
ADD COLUMN IF NOT EXISTS slug VARCHAR(255);

-- Generate slug from display_name for existing knowledge_base records
-- Slug is URL-safe: lowercase, alphanumeric with hyphens only
UPDATE knowledge_base
SET slug = LOWER(
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(display_name, '[^a-zA-Z0-9\s-]', '', 'g'),
            '\s+',
            '-',
            'g'
        ),
        '-+',
        '-',
        'g'
    )
)
WHERE slug IS NULL OR slug = '';

-- Generate slug from display_name for existing file records
UPDATE file
SET slug = LOWER(
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(display_name, '[^a-zA-Z0-9\s-]', '', 'g'),
            '\s+',
            '-',
            'g'
        ),
        '-+',
        '-',
        'g'
    )
)
WHERE slug IS NULL OR slug = '';

-- Create indexes for slug lookups (performance optimization)
CREATE INDEX IF NOT EXISTS idx_knowledge_base_slug ON knowledge_base(slug);
CREATE INDEX IF NOT EXISTS idx_file_slug ON file(slug);
