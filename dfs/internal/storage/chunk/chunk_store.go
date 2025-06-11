package chunk

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type ChunkDiskStorage struct {
	config DiskStorageConfig
}

func NewChunkDiskStorage(config DiskStorageConfig) (*ChunkDiskStorage, error) {
	if err := os.MkdirAll(config.RootDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create rootDir for chunk disk storage: %w", err)
	}
	log.Printf("Created chunk disk storage rootDir: %s", config.RootDir)
	return &ChunkDiskStorage{config: config}, nil
}

// validateChunkID checks if the chunk ID is in the correct format
func (d *ChunkDiskStorage) validateChunkID(chunkID string) error {
	parts := strings.Split(chunkID, "_")
	if len(parts) != 2 {
		return fmt.Errorf("invalid chunk ID format: %s", chunkID)
	}

	// First part should be a hex string (8 bytes = 16 hex chars)
	if len(parts[0]) != 16 {
		return fmt.Errorf("invalid path hash in chunk ID: %s", chunkID)
	}

	// Second part should be a number
	if _, err := fmt.Sscanf(parts[1], "%d", new(int)); err != nil {
		return fmt.Errorf("invalid chunk number in ID: %s", chunkID)
	}

	return nil
}

// getChunkPath generates a nested path from a chunkID to prevent having too many files in a single directory.
// For example, a chunkID of "f1d2d2f924e9..." will be stored at "<rootDir>/f1/d2/f1d2d2f924e9...".
func (d *ChunkDiskStorage) getChunkPath(chunkID string) (string, error) {
	if err := d.validateChunkID(chunkID); err != nil {
		return "", err
	}

	// Use the first 4 characters of the path hash to create 2 levels of directories
	dir1 := chunkID[0:2]
	dir2 := chunkID[2:4]
	return filepath.Join(d.config.RootDir, dir1, dir2, chunkID), nil
}

func (d *ChunkDiskStorage) Store(chunkID string, data []byte) error {
	fullPath, err := d.getChunkPath(chunkID)
	if err != nil {
		return err
	}

	// Extract the directory path from the full file path
	dirPath := filepath.Dir(fullPath)

	// Create all necessary directories (including nested ones)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory structure %s: %w", dirPath, err)
	}

	// Now write the file
	if err := os.WriteFile(fullPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write chunk file %s: %w", fullPath, err)
	}

	log.Printf("Stored chunk %s at %s", chunkID, fullPath)
	return nil
}

func (d *ChunkDiskStorage) Retrieve(chunkID string) ([]byte, error) {
	fullPath, err := d.getChunkPath(chunkID)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk file %s: %w", fullPath, err)
	}

	log.Printf("Retrieved chunk %s from %s", chunkID, fullPath)
	return data, nil
}

func (d *ChunkDiskStorage) Delete(chunkID string) error {
	fullPath, err := d.getChunkPath(chunkID)
	if err != nil {
		return err
	}
	if err := os.Remove(fullPath); err != nil {
		return err
	}

	// Clean up empty parent directories.
	dirPath := filepath.Dir(fullPath)
	for i := 0; i < 2; i++ { // we have 2 levels of directories.
		empty, err := isDirEmpty(dirPath)
		if err != nil {
			// Log error but don't fail the whole operation,
			// as the file is already deleted.
			log.Printf("failed to check if dir is empty %s: %v", dirPath, err)
			return nil
		}
		if !empty {
			break
		}
		if err := os.Remove(dirPath); err != nil {
			log.Printf("failed to remove empty dir %s: %v", dirPath, err)
			return nil
		}
		dirPath = filepath.Dir(dirPath)
	}

	return nil
}

func (d *ChunkDiskStorage) Exists(chunkID string) bool {
	fullPath, err := d.getChunkPath(chunkID)
	if err != nil {
		return false
	}
	_, err = os.Stat(fullPath)
	return !errors.Is(err, fs.ErrNotExist)
}

func (d *ChunkDiskStorage) List() ([]string, error) {
	var chunks []string

	if err := filepath.WalkDir(d.config.RootDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !entry.IsDir() {
			chunks = append(chunks, entry.Name())
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return chunks, nil
}

// isDirEmpty checks if a directory is empty.
func isDirEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	// Read exactly one directory entry.
	// If we get an io.EOF error, the directory is empty.
	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	// If there is no error, it means we read an entry, so dir is not empty.
	// Otherwise, we return the error.
	return false, err
}
