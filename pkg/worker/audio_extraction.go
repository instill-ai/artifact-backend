package worker

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/ai/gemini"
)

// ---------------------------------------------------------------------------
// ExtractAudioActivity
// ---------------------------------------------------------------------------

// ExtractAudioActivityParam defines input for ExtractAudioActivity.
type ExtractAudioActivityParam struct {
	Bucket           string  // MinIO bucket containing the video
	SourcePath       string  // MinIO path to the standardized video
	FileUID          string
	WorkflowRunID    string
	VideoDurationSec float64 // exact video duration from ffprobe, used to align audio
}

// ExtractAudioActivityResult defines output from ExtractAudioActivity.
type ExtractAudioActivityResult struct {
	HasAudio       bool   // false if the video has no audio stream
	AudioMinIOPath string // MinIO path of the extracted audio (empty when HasAudio=false)
	AudioMIMEType  string // e.g. "audio/mp4"
}

// ExtractAudioActivity downloads a video from MinIO, extracts the audio
// track with FFmpeg, and uploads the result back to MinIO.
// If the video has no audio stream, it returns HasAudio=false.
func (w *Worker) ExtractAudioActivity(ctx context.Context, param *ExtractAudioActivityParam) (*ExtractAudioActivityResult, error) {
	logger := w.log.With(
		zap.String("fileUID", param.FileUID),
		zap.String("sourcePath", param.SourcePath))
	logger.Info("ExtractAudioActivity: starting")

	content, err := w.repository.GetMinIOStorage().GetFile(ctx, param.Bucket, param.SourcePath)
	if err != nil {
		return nil, fmt.Errorf("download video for audio extraction: %w", err)
	}

	tmpVideo, err := os.CreateTemp("", "extract-audio-in-*.mp4")
	if err != nil {
		return nil, fmt.Errorf("create temp video file: %w", err)
	}
	defer os.Remove(tmpVideo.Name())

	if _, err := tmpVideo.Write(content); err != nil {
		tmpVideo.Close()
		return nil, fmt.Errorf("write temp video: %w", err)
	}
	tmpVideo.Close()

	// Probe for audio stream first.
	probeOut, err := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-select_streams", "a",
		"-show_entries", "stream=codec_type",
		"-of", "csv=p=0",
		tmpVideo.Name(),
	).CombinedOutput()
	if err != nil || strings.TrimSpace(string(probeOut)) == "" {
		logger.Info("ExtractAudioActivity: no audio stream detected")
		return &ExtractAudioActivityResult{HasAudio: false}, nil
	}

	tmpAudio, err := os.CreateTemp("", "extract-audio-out-*.m4a")
	if err != nil {
		return nil, fmt.Errorf("create temp audio file: %w", err)
	}
	tmpAudioPath := tmpAudio.Name()
	tmpAudio.Close()
	defer os.Remove(tmpAudioPath)

	// Stream-copy the audio track to preserve exact packet timing.
	// Re-encoding (e.g. to AAC) introduces priming samples, encoder
	// padding, and sample-rate conversion drift that shift the audio
	// relative to the video — the primary cause of A/V sync drift in
	// downstream transcription timestamps.
	// Fallback: if stream copy fails (unusual codec or container
	// incompatibility), re-encode to AAC with -async 1 to sync the
	// audio start time.
	args := []string{"-i", tmpVideo.Name(), "-vn", "-c:a", "copy", "-y", tmpAudioPath}
	if out, err := exec.CommandContext(ctx, "ffmpeg", args...).CombinedOutput(); err != nil {
		logger.Warn("ExtractAudioActivity: stream copy failed, falling back to AAC re-encode",
			zap.Error(err), zap.String("ffmpegOutput", string(out)))
		args = []string{"-i", tmpVideo.Name(), "-vn", "-c:a", "aac", "-async", "1"}
		if param.VideoDurationSec > 0 {
			args = append(args, "-t", fmt.Sprintf("%.3f", param.VideoDurationSec))
		}
		args = append(args, "-y", tmpAudioPath)
		if out2, err2 := exec.CommandContext(ctx, "ffmpeg", args...).CombinedOutput(); err2 != nil {
			return nil, fmt.Errorf("ffmpeg audio extraction failed: %w\noutput: %s", err2, string(out2))
		}
	}

	audioData, err := os.ReadFile(tmpAudioPath)
	if err != nil {
		return nil, fmt.Errorf("read extracted audio: %w", err)
	}

	ext := filepath.Ext(tmpAudioPath)
	mimeType := "audio/mp4"
	if ext == ".ogg" {
		mimeType = "audio/ogg"
	}

	minioPath := fmt.Sprintf("temp/audio/%s/%s/extracted-audio%s", param.WorkflowRunID, param.FileUID, ext)
	b64 := base64.StdEncoding.EncodeToString(audioData)
	if err := w.repository.GetMinIOStorage().UploadBase64File(ctx, param.Bucket, minioPath, b64, mimeType); err != nil {
		return nil, fmt.Errorf("upload extracted audio to MinIO: %w", err)
	}

	logger.Info("ExtractAudioActivity: complete",
		zap.String("audioPath", minioPath),
		zap.Int("audioSize", len(audioData)))

	return &ExtractAudioActivityResult{
		HasAudio:       true,
		AudioMinIOPath: minioPath,
		AudioMIMEType:  mimeType,
	}, nil
}

// ---------------------------------------------------------------------------
// UploadToGCSActivity
// ---------------------------------------------------------------------------

// UploadToGCSActivityParam defines input for UploadToGCSActivity.
type UploadToGCSActivityParam struct {
	Bucket     string // MinIO bucket
	SourcePath string // MinIO path of the file to upload
	MIMEType   string
	FileUID    string
}

// UploadToGCSActivityResult defines output from UploadToGCSActivity.
type UploadToGCSActivityResult struct {
	GSURI         string // gs://bucket/path
	GCSObjectPath string // for cleanup
}

// UploadToGCSActivity downloads a file from MinIO and uploads it to GCS
// via the AI client's storage, returning the gs:// URI.
func (w *Worker) UploadToGCSActivity(ctx context.Context, param *UploadToGCSActivityParam) (*UploadToGCSActivityResult, error) {
	logger := w.log.With(zap.String("fileUID", param.FileUID), zap.String("source", param.SourcePath))
	logger.Info("UploadToGCSActivity: starting")

	content, err := w.repository.GetMinIOStorage().GetFile(ctx, param.Bucket, param.SourcePath)
	if err != nil {
		return nil, fmt.Errorf("download file from MinIO for GCS upload: %w", err)
	}

	objectPath := fmt.Sprintf("audio-transcription/%s/%s", param.FileUID, filepath.Base(param.SourcePath))
	gsURI, err := w.aiClient.UploadToGCS(ctx, content, objectPath, param.MIMEType)
	if err != nil {
		return nil, fmt.Errorf("upload to GCS: %w", err)
	}

	logger.Info("UploadToGCSActivity: complete",
		zap.String("gsURI", gsURI),
		zap.Int("size", len(content)))

	return &UploadToGCSActivityResult{
		GSURI:         gsURI,
		GCSObjectPath: objectPath,
	}, nil
}

// ---------------------------------------------------------------------------
// TranscribeAudioActivity
// ---------------------------------------------------------------------------

// TranscribeAudioActivityParam defines input for TranscribeAudioActivity.
type TranscribeAudioActivityParam struct {
	GSURI          string
	MIMEType       string
	FileUID        string
	WorkflowRunID  string
	SpeakerContext string // optional, from IdentifySpeakersActivity
}

// TranscribeAudioActivityResult defines output from TranscribeAudioActivity.
type TranscribeAudioActivityResult struct {
	Bucket        string
	TempMinIOPath string
	UsageMetadata map[string]interface{}
	Model         string
}

// TranscribeAudioActivity calls ConvertAudioDirect with the audio GCS URI
// and writes the transcript to MinIO.
func (w *Worker) TranscribeAudioActivity(ctx context.Context, param *TranscribeAudioActivityParam) (*TranscribeAudioActivityResult, error) {
	logger := w.log.With(
		zap.String("fileUID", param.FileUID),
		zap.String("gsURI", param.GSURI))
	logger.Info("TranscribeAudioActivity: starting")

	prompt := w.getAudioTranscriptionPrompt()
	if param.SpeakerContext != "" {
		prompt = param.SpeakerContext + "\n\n" + prompt
	}

	result, err := w.aiClient.ConvertAudioDirect(ctx, param.GSURI, param.MIMEType, prompt)
	if err != nil {
		return nil, fmt.Errorf("ConvertAudioDirect: %w", err)
	}

	tempPath := fmt.Sprintf("temp/audio-transcript/%s/%s/transcript.md",
		param.WorkflowRunID, param.FileUID)

	b64 := base64.StdEncoding.EncodeToString([]byte(result.Markdown))
	bucket := config.Config.Minio.BucketName
	if err := w.repository.GetMinIOStorage().UploadBase64File(ctx, bucket, tempPath, b64, "text/markdown"); err != nil {
		return nil, fmt.Errorf("upload audio transcript to MinIO: %w", err)
	}

	acc := &usageAccumulator{}
	acc.Add(result.UsageMetadata, result.Model)

	logger.Info("TranscribeAudioActivity: complete",
		zap.Int("markdownLen", len(result.Markdown)),
		zap.String("tempPath", tempPath))

	return &TranscribeAudioActivityResult{
		Bucket:        bucket,
		TempMinIOPath: tempPath,
		UsageMetadata: acc.ToMap(),
		Model:         result.Model,
	}, nil
}

// getAudioTranscriptionPrompt returns the speech-only audio prompt.
// Resolution: per-modality override for "audio_transcription" → CE fallback.
func (w *Worker) getAudioTranscriptionPrompt() string {
	if w.generateContentPrompts != nil {
		if p, ok := w.generateContentPrompts["audio_transcription"]; ok {
			return p
		}
	}
	return gemini.GetGenerateContentAudioTranscriptionPrompt()
}

// ---------------------------------------------------------------------------
// DeleteFromGCSActivity
// ---------------------------------------------------------------------------

// DeleteFromGCSActivityParam defines input for DeleteFromGCSActivity.
type DeleteFromGCSActivityParam struct {
	GCSObjectPath string
}

// DeleteFromGCSActivity removes a file from GCS.
func (w *Worker) DeleteFromGCSActivity(ctx context.Context, param *DeleteFromGCSActivityParam) error {
	w.log.Info("DeleteFromGCSActivity: cleaning up", zap.String("path", param.GCSObjectPath))
	return w.aiClient.DeleteFromGCS(ctx, param.GCSObjectPath)
}

// ---------------------------------------------------------------------------
// ReadMinIOFileActivity
// ---------------------------------------------------------------------------

// ReadMinIOFileActivityParam defines input for ReadMinIOFileActivity.
type ReadMinIOFileActivityParam struct {
	Bucket string
	Path   string
}

// ReadMinIOFileActivityResult defines output from ReadMinIOFileActivity.
type ReadMinIOFileActivityResult struct {
	Content string
}

// ReadMinIOFileActivity reads a text file from MinIO and returns its content.
func (w *Worker) ReadMinIOFileActivity(ctx context.Context, param *ReadMinIOFileActivityParam) (*ReadMinIOFileActivityResult, error) {
	data, err := w.repository.GetMinIOStorage().GetFile(ctx, param.Bucket, param.Path)
	if err != nil {
		return nil, fmt.Errorf("read file from MinIO: %w", err)
	}
	return &ReadMinIOFileActivityResult{Content: string(data)}, nil
}

// ---------------------------------------------------------------------------
// WriteMinIOFileActivity
// ---------------------------------------------------------------------------

// WriteMinIOFileActivityParam defines input for WriteMinIOFileActivity.
type WriteMinIOFileActivityParam struct {
	Bucket  string
	Path    string
	Content string
}

// WriteMinIOFileActivity overwrites a text file in MinIO with new content.
func (w *Worker) WriteMinIOFileActivity(ctx context.Context, param *WriteMinIOFileActivityParam) error {
	b64 := base64.StdEncoding.EncodeToString([]byte(param.Content))
	if err := w.repository.GetMinIOStorage().UploadBase64File(ctx, param.Bucket, param.Path, b64, "text/markdown"); err != nil {
		return fmt.Errorf("write file to MinIO: %w", err)
	}
	return nil
}
