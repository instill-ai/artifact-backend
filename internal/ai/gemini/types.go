package gemini

import (
	"time"
)

// ConversionInput defines the input for content-to-markdown conversion (supports documents, images, audio, video)
// Note: This is for direct conversion only. For cached conversion, use ConvertToMarkdownWithCache() separately.
type ConversionInput struct {
	// Content is the file content (raw bytes for any supported type)
	Content []byte
	// ContentType is the MIME type of the file (e.g., image/jpeg, video/mp4, application/pdf)
	ContentType string
	// Filename is the original filename (helps with format detection)
	Filename string
	// Model specifies the Gemini model to use (default: gemini-2.5-flash)
	Model string
	// CustomPrompt allows overriding the default conversion prompt
	CustomPrompt *string
}

// ConversionOutput defines the output of content-to-markdown conversion
type ConversionOutput struct {
	// Markdown is the converted/extracted markdown content
	Markdown string
	// TokensUsed tracks the number of tokens consumed
	TokensUsed *TokenUsage
	// CacheName is the name of the cached content (if caching was enabled)
	CacheName *string
	// Model is the model that was used for conversion
	Model string
}

// TokenUsage tracks token consumption for billing and monitoring
type TokenUsage struct {
	InputTokens  int32
	OutputTokens int32
	TotalTokens  int32
	// CachedInputTokens tracks tokens read from cache (much cheaper)
	CachedInputTokens int32
}

// CacheInfo contains information about a cached content conversion
type CacheInfo struct {
	CacheName  string
	Model      string
	CreateTime time.Time
	ExpireTime time.Time
	// ContentType of the cached content (document, image, audio, or video)
	ContentType string
}

// DefaultConversionModel is the default Gemini model for multimodal content conversion
const DefaultConversionModel = "gemini-2.5-flash"

// DefaultCacheTTL is the default time-to-live for cached content (1 hour)
var DefaultCacheTTL = time.Hour

// DefaultSystemInstruction is the default system instruction for multimodal content understanding
const DefaultSystemInstruction = "You are an AI data pre-processor. Your sole function is to convert multimodal content (documents, images, audio, video) into clean, well-structured, RAG-optimized Markdown."

// DefaultPromptTemplate is the default prompt for multimedia-to-markdown conversion
const DefaultPromptTemplate = `To produce a complete and accurate Markdown representation of the source content, preserving all information and its original structure. The output will be programmatically chunked and embedded for a semantic search system, so accuracy and strict adherence to format are critical.

---

### **Core Rules**

1. **Completeness**: Absolutely no information should be lost. All text, visual data, and spoken words must be captured.
2. **Literal Transcription**: Transcribe content verbatim. Do **NOT** summarize, interpret, add information, or speculate.
3. **Structure**: Precisely replicate the logical hierarchy and flow of the original content using semantic Markdown.

---

### **Markdown Formatting**

* **Headings**: Use \#, \#\#, \#\#\# to represent the document's heading structure.
* **Text Styles**: Preserve **bold**, *italic*, and monospace formatting.
* **Lists**: Maintain numbered (1.) and bulleted (\* or \-) lists, including nested structures.
* **Tables**: Format tables using valid Markdown syntax.
  Example:
  | Header 1 | Header 2 |
  |----------|----------|
  | Cell 1   | Cell 2   |

* **Separators**: Use a horizontal rule (---) to denote significant semantic breaks, such as page breaks in a document.

---

### **Modality-Specific Instructions**

#### **Visual Content (Images, Charts, Diagrams)**

* **General Images**: Provide a concise but comprehensive description within a tag. Describe all key objects, people, settings, and any visible text.
  * **Format**: \[Image: A description of the image's content.\]
* **Charts & Diagrams**: Describe the type of visual, its purpose, axes, data points, and the key trend or information it conveys.
  * **Format**: \[Chart: A \[type of chart, e.g., bar chart\] showing \[data represented\]. The X-axis is \[label\], the Y-axis is \[label\]. Key data points include...\]

#### **Audio & Video Content**

* **Speech Transcription**: Transcribe all spoken words accurately. Use timestamps for clarity.
  * **Format**: \[Audio: HH:MM:SS\] Speaker Name: \<Transcript of speech\>
* **Non-Verbal Audio**: Note significant background noises or sounds relevant to the context.
  * **Format**: \[Sound: HH:MM:SS\] \<Description of sound, e.g., phone ringing, door closing\>
* **Video Visuals**: Describe important visual events, actions, or on-screen text in the video feed.
  * **Format**: \[Video: HH:MM:SS\] \<Description of visual events on screen\>

---

### **CRITICAL: Output Format**

* You **MUST** return only the raw Markdown text.
* You **MUST NOT** wrap your response in Markdown code blocks (e.g., fenced code blocks with "markdown" language tag).
* You **MUST NOT** include any conversational text, preamble, explanations, or sign-offs (e.g., "Here is the converted Markdown:"). Your response should begin directly with the first line of the converted content.

---

**Convert the provided content now:**`
