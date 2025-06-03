# Basic RAG Pipeline (Indexing Step 2: Splitting Template)

## Pipeline Overview

This pipeline is designed to demonstrate the second step of the RAG indexing phase, which involves splitting text into smaller chunks. This pipeline takes a single text (from a previous step pipeline such as [indexing-extract](https://instill-ai.com/leochen5/pipelines/indexing-extract)) and splits it into smaller chunks based on specified split parameters. This makes it a default pipeline for Step 2 in a Knowledge Base application (text -> chunks).

This example uses a **recursive** strategy (ref: [recursive_text_splitter](https://python.langchain.com/v0.2/docs/how_to/recursive_text_splitter/)) to split long text.

## How to Use

To use this pipeline, you simply need to provide a text as input, along with the parameters needed for the chunking splitter. The pipeline will then split the text into smaller chunks.

**Input:**

* `text-input`: Extracted text from Step 1 indexing-extract or input a text string
* `max-chunk-length`: Specifies the maximum size of each chunk in terms of the number of tokens.
* `chunk-overlap`: Determines the number of tokens that overlap between consecutive chunks.

**Output:**

* `split-result`: Split chunks from the input text (Text Chunk: array of jsons)
  * `Text`: A chunk from the input text (string)
  * `Start Position`: The starting position of the chunk in the original text (number)
  * `End Position`: The ending position of the chunk in the original text (number)

For more detailed information or if you need any customized assistance with any kind of complex RAG use cases or if you want to introduce it into real-world complex application scenarios, feel free to book a meeting with us: [30min-talk](https://cal.com/instill-ai/30min-talk)
