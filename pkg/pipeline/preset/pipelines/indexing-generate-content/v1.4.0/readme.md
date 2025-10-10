# Advanced Document Conversion Pipeline v1.4.0

## Overview

This pipeline converts various page-based document formats (PDF, DOCX, DOC, PPT, PPTX) into markdown, respecting the page information. It's designed for advanced RAG (Retrieval-Augmented Generation) systems that require precise page-level document processing.

## Key Features

- **Page-based Processing**: Converts documents into arrays of markdown strings, one per page
- **VLM Enhancement**: Uses Visual Language Models to improve OCR and text extraction quality
- **Fallback Strategy**: Combines heuristic conversion with VLM refinement for optimal results

## Pipeline Flow

1. **Document Conversion**: Converts input document to both markdown and images
3. **VLM Processing**: Uses OpenAI's GPT-4o to enhance text extraction and OCR
4. **Result Collection**: Returns arrays of page-based markdown strings

## Input Variables

- `document_input`: The document file to be converted (PDF/DOCX/DOC/PPTX/PPT)

## Output Fields

- `convert_result`: Array of markdown strings (one per page) from VLM refinement
- `convert_result2`: Array of markdown strings (one per page) from VLM OCR
