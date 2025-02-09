# Basic RAG Pipeline (Answer User Queries Using LLM with Retrieved Context)

## Pipeline Overview

This pipeline is primarily used with the Artifact component or can be called using a low-code API for QnA. It utilizes the input chunk information and query question to answer user queries using an LLM. This pipeline serves as a demo purpose, with prompts developed for general use cases. You can clone this pipeline to adjust the required input and output formats and further refine and test the prompts to meet your specific needs.

## How to Use

**Input:**

* `user-question` (string): User query
* `retrieved-chunk` (string): All chunk information retrieved from the vector database
  * Combine the retrieved chunk text information using \n\n

**Output:**

1. `assistant-reply` (string): The answer provided by the LLM

**Note:** You can also use a low-code approach to interface with your own system backend. You just need to rewrite the curl command in the toolkit in a suitable programming language.

For more detailed information or assistance with similar use cases or if you want to introduce it into real-world complex application scenarios, feel free to book a meeting with us: [https://cal.com/instill-ai/30min-talk](https://cal.com/instill-ai/30min-talk)
