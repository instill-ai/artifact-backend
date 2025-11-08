package vertexai

// Config holds VertexAI client configuration
type Config struct {
	// ProjectID is the GCP project ID for VertexAI
	ProjectID string
	// Region is the GCP region for VertexAI API (e.g., "us-central1")
	Region string
	// SAKey is the JSON string of the service account key
	SAKey string
}
