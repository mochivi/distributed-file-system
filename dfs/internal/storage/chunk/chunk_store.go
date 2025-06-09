package chunk

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
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

func (d *ChunkDiskStorage) Store(chunkID string, data []byte) error {
	// Create the full file path
	fullPath := filepath.Join(d.config.RootDir, chunkID)

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
	return nil
}

func (d *ChunkDiskStorage) Retrieve(chunkID string) ([]byte, error) {
	return os.ReadFile(filepath.Join(d.config.RootDir, chunkID))
}

func (d *ChunkDiskStorage) Delete(chunkID string) error {
	return os.Remove(filepath.Join(d.config.RootDir, chunkID))
}

func (d *ChunkDiskStorage) Exists(chunkID string) bool {
	_, err := os.Stat(filepath.Join(d.config.RootDir, chunkID))
	return !errors.Is(err, fs.ErrNotExist)
}

func (d *ChunkDiskStorage) List() ([]string, error) {
	var chunks []string

	if err := filepath.WalkDir(d.config.RootDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !entry.IsDir() {
			relPath, err := filepath.Rel(d.config.RootDir, path)
			if err != nil {
				return err
			}
			chunks = append(chunks, string(filepath.Separator)+relPath)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return chunks, nil
}
