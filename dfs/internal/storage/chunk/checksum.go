package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
)

func CalculateChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
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
