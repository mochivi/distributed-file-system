package common

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// FormatChunkID creates a unique identifier for a chunk using a hash of the file path
// and the chunk number. This avoids issues with long paths and special characters.
func FormatChunkID(path string, chunkID int) string {
	// Create a hash of the file path to avoid long filenames
	pathHash := sha256.Sum256([]byte(path))
	pathHashStr := hex.EncodeToString(pathHash[:8]) // Use first 8 bytes for shorter ID
	return fmt.Sprintf("%s_%d", pathHashStr, chunkID)
}

// ParseChunkID extracts the path hash and chunk number from a chunk ID
func ParseChunkID(chunkID string) (string, int, error) {
	parts := strings.Split(chunkID, "_")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid chunk ID format: %s", chunkID)
	}

	chunkNum, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid chunk number in ID: %s", chunkID)
	}

	return parts[0], chunkNum, nil
}

func CalculateChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func CalculateFileChecksumFromPath(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// Reads and resets the file pointer
func CalculateFileChecksum(file *os.File) (string, error) {
	// Save current position
	currentPos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", err
	}

	// Go to beginning for checksum calculation
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	// Reset to original position
	if _, err := file.Seek(currentPos, io.SeekStart); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func VerifyChecksum(data []byte, expected string) bool {
	actual := CalculateChecksum(data)
	return actual == expected
}
