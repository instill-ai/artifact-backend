# Basic RAG Pipeline (Indexing Step 2: Splitting Template)

## Pipeline Overview

This pipeline is designed to demonstrate the second step of the RAG indexing phase, which involves splitting text into smaller chunks. This pipeline takes a single markdown string (from a previous step pipeline such as [indexing-convert-pdf](https://instill-ai.com/leochen5/pipelines/indexing-convert-pdf)) and splits it into smaller chunks based on specified split parameters. This makes it a default pipeline for Step 2 in a Knowledge Base application (markdown -> chunks).

This example uses the Markdown strategy (ref: [markdown_header_metadata](https://python.langchain.com/v0.1/docs/modules/data_connection/document_transformers/markdown_header_metadata/)) to split a long markdown string.

## How to Use

To use this pipeline, you simply need to provide a markdown string as input, along with the parameters needed for the chunking splitter. The pipeline will then split the markdown string into smaller chunks.

**Input:**

* `md-input`: Extracted text from Step 1 [indexing-convert-pdf](https://instill-ai.com/leochen5/pipelines/indexing-convert-pdf) or input a markdown string
* `max-chunk-length`: Specifies the maximum size of each chunk in terms of the number of tokens.
* `chunk-overlap`: Determines the number of tokens that overlap between consecutive chunks.

**Output:**

* `split-result`: Split chunks from the input markdown (Text Chunk: array of jsons)
  * `Text`: A chunk from the input text (string)
  * `Start Position`: The starting position of the chunk in the original markdown (number)
  * `End Position`: The ending position of the chunk in the original markdown (number)

For more detailed information or if you need any customized assistance with any kind of complex RAG use cases or if you want to introduce it into real-world complex application scenarios, feel free to [book a meeting with us](https://cal.com/instill-ai/30min-talk).
