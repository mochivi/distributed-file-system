package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// FormatChunkID creates a unique identifier for a chunk using a hash of the file path
// and the chunk number. This avoids issues with long paths and special characters.
func FormatChunkID(path string, chunkIndex int) string {
	// Create a hash of the file path to avoid long filenames
	pathHash := sha256.Sum256([]byte(path))
	pathHashStr := hex.EncodeToString(pathHash[:8]) // Use first 8 bytes for shorter ID
	return fmt.Sprintf("%s_%d", pathHashStr, chunkIndex)
}

func FormatChunkIDs(prefix string, numChunks int) []string {
	chunkIDs := make([]string, numChunks)
	for i := range numChunks {
		chunkID := fmt.Sprintf("%s_%d", prefix, i)
		chunkIDs[i] = chunkID
	}
	return chunkIDs
}

func HashFilepath(path string) string {
	pathHash := sha256.Sum256([]byte(path))
	pathHashStr := hex.EncodeToString(pathHash[:8]) // Use first 8 bytes for shorter ID
	return pathHashStr
}

// ParseChunkID extracts the path hash and chunk number from a chunk ID
func ParseChunkID(chunkID string) (string, int, error) {
	parts := strings.Split(chunkID, "_")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("%w: invalid format: %s", ErrInvalidChunkID, chunkID)
	}

	chunkIndex, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("%w: invalid chunk index: %s", ErrInvalidChunkID, chunkID)
	}

	return parts[0], chunkIndex, nil
}
