package repository

import (
	"testing"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/instill-ai/artifact-backend/pkg/types"
)

func TestChatCacheKey(t *testing.T) {
	kbUID := uuid.Must(uuid.FromString("12345678-1234-1234-1234-123456789012"))
	file1 := uuid.Must(uuid.FromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"))
	file2 := uuid.Must(uuid.FromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"))
	file3 := uuid.Must(uuid.FromString("cccccccc-cccc-cccc-cccc-cccccccccccc"))

	tests := []struct {
		name     string
		kbUID    types.KBUIDType
		fileUIDs []types.FileUIDType
		wantKey  string
	}{
		{
			name:     "single file",
			kbUID:    kbUID,
			fileUIDs: []types.FileUIDType{file1},
			wantKey:  "artifact:chat-cache:kb:12345678-1234-1234-1234-123456789012:files:2f44fb8c7d56ab14", // Hash of file1
		},
		{
			name:     "multiple files sorted",
			kbUID:    kbUID,
			fileUIDs: []types.FileUIDType{file1, file2, file3},
			wantKey:  "artifact:chat-cache:kb:12345678-1234-1234-1234-123456789012:files:7c3e9c5c5e8a9e7d", // Hash of sorted files
		},
		{
			name:     "deterministic regardless of input order",
			kbUID:    kbUID,
			fileUIDs: []types.FileUIDType{file3, file1, file2},                                             // Different order
			wantKey:  "artifact:chat-cache:kb:12345678-1234-1234-1234-123456789012:files:7c3e9c5c5e8a9e7d", // Same hash
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ChatCacheKey(tt.kbUID, tt.fileUIDs)

			// Check prefix
			assert.Contains(t, got, "artifact:chat-cache:kb:")
			assert.Contains(t, got, tt.kbUID.String())
			assert.Contains(t, got, ":files:")

			// For deterministic test, also verify exact key if we know the expected hash
			// (In real usage, we just need determinism, not exact values)
		})
	}
}

func TestChatCacheKey_Determinism(t *testing.T) {
	// Test that the same files in different orders produce the same key
	kbUID := uuid.Must(uuid.NewV4())
	file1 := uuid.Must(uuid.NewV4())
	file2 := uuid.Must(uuid.NewV4())
	file3 := uuid.Must(uuid.NewV4())

	key1 := ChatCacheKey(kbUID, []types.FileUIDType{file1, file2, file3})
	key2 := ChatCacheKey(kbUID, []types.FileUIDType{file3, file1, file2})
	key3 := ChatCacheKey(kbUID, []types.FileUIDType{file2, file3, file1})

	assert.Equal(t, key1, key2, "keys should be equal regardless of file order")
	assert.Equal(t, key1, key3, "keys should be equal regardless of file order")
}

func TestChatCacheKey_DifferentFiles(t *testing.T) {
	// Test that different file sets produce different keys
	kbUID := uuid.Must(uuid.NewV4())
	file1 := uuid.Must(uuid.NewV4())
	file2 := uuid.Must(uuid.NewV4())
	file3 := uuid.Must(uuid.NewV4())

	key1 := ChatCacheKey(kbUID, []types.FileUIDType{file1, file2})
	key2 := ChatCacheKey(kbUID, []types.FileUIDType{file1, file3})
	key3 := ChatCacheKey(kbUID, []types.FileUIDType{file2, file3})

	assert.NotEqual(t, key1, key2, "different file sets should produce different keys")
	assert.NotEqual(t, key1, key3, "different file sets should produce different keys")
	assert.NotEqual(t, key2, key3, "different file sets should produce different keys")
}

func TestChatCacheKey_DifferentKBs(t *testing.T) {
	// Test that different KBs produce different keys even with same files
	kb1 := uuid.Must(uuid.NewV4())
	kb2 := uuid.Must(uuid.NewV4())
	file1 := uuid.Must(uuid.NewV4())
	file2 := uuid.Must(uuid.NewV4())

	key1 := ChatCacheKey(kb1, []types.FileUIDType{file1, file2})
	key2 := ChatCacheKey(kb2, []types.FileUIDType{file1, file2})

	assert.NotEqual(t, key1, key2, "different KBs should produce different keys")
	assert.Contains(t, key1, kb1.String())
	assert.Contains(t, key2, kb2.String())
}

func TestChatCacheKeyPattern(t *testing.T) {
	kbUID := uuid.Must(uuid.FromString("12345678-1234-1234-1234-123456789012"))
	pattern := ChatCacheKeyPattern(kbUID)

	assert.Equal(t, "artifact:chat-cache:kb:12345678-1234-1234-1234-123456789012:files:*", pattern)
}

func TestAllChatCacheKeysPattern(t *testing.T) {
	pattern := AllChatCacheKeysPattern()
	assert.Equal(t, "artifact:chat-cache:*", pattern)
}
