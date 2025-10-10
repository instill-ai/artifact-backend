# Basic RAG Pipeline (Indexing Step 3: Vectorization Template)

## Pipeline Overview

This pipeline is designed to demonstrate the third step of the RAG indexing phase, which involves converting each text chunk into an embedding vector for storage in a vector database. This pipeline takes a single chunk string (from a previous step pipeline such as [indexing-split-markdown](https://instill-ai.com/leochen5/pipelines/indexing-split-markdown) or [indexing-split-text](https://instill-ai.com/leochen5/pipelines/indexing-split-text)) and embeds each small chunk into a vector. This makes it a default pipeline for Step 3 in a Knowledge Base application (chunk -> vector).

In this example, we use the OpenAI embedding model (text-embedding-3-small) to vectorize the text string, resulting in a 1536-dimension vector.

## How to Use

To use this pipeline, you simply need to provide a chunk string as input. The pipeline will then embed the input string into a vector.

**Input:**

* `chunk-input`: Extracted text from Step 2 ([indexing-split-markdown](https://instill-ai.com/leochen5/pipelines/indexing-split-markdown) or [indexing-split-text](https://instill-ai.com/leochen5/pipelines/indexing-split-text)) or directly input a text string to test

**Output:**

* `embed-result`: Embed a chunk into a vector using the OpenAI embedding model (array of numbers)

For more detailed information or if you need any customized assistance with any kind of complex RAG use cases or if you want to introduce it into real-world complex application scenarios, feel free to [book a meeting with us](https://cal.com/instill-ai/30min-talk).
