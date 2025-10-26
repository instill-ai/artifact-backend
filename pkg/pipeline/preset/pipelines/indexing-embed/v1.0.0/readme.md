# Indexing Embed Pipeline

This pipeline generates embeddings using OpenAI's text-embedding-3-small model.

## Purpose

Used during document indexing to convert text chunks into 1536-dimensional embedding vectors for semantic search in the vector database.

## Model

- **Model**: text-embedding-3-small
- **Dimensions**: 1536 (fixed)
- **Provider**: OpenAI

## Usage

This pipeline is automatically invoked by the artifact-backend when:
- Knowledge base is configured with OpenAI model family
- Text chunks need to be embedded during file processing

## Input

- `text`: The text chunk to embed

## Output

- `embedding`: 1536-dimensional embedding vector
