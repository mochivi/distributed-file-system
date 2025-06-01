package chunk

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

type ChunkDiskStorage struct {
	rootDir string
}

func NewChunkDiskStorage(rootDir string) *ChunkDiskStorage {
	os.Mkdir(rootDir, 0755)
	return &ChunkDiskStorage{rootDir: rootDir}
}

func (d *ChunkDiskStorage) Store(chunkID string, data []byte) error {
	return os.WriteFile(filepath.Join(d.rootDir, chunkID), data, 0644)
}

func (d *ChunkDiskStorage) Retrieve(chunkID string) ([]byte, error) {
	return os.ReadFile(filepath.Join(d.rootDir, chunkID))
}

func (d *ChunkDiskStorage) Delete(chunkID string) error {
	return os.Remove(filepath.Join(d.rootDir, chunkID))
}

func (d *ChunkDiskStorage) Exists(chunkID string) bool {
	_, err := os.Stat(filepath.Join(d.rootDir, chunkID))
	return !errors.Is(err, fs.ErrNotExist)
}

func (d *ChunkDiskStorage) List() ([]string, error) {
	var chunks []string

	if err := filepath.WalkDir(d.rootDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !entry.IsDir() {
			relPath, err := filepath.Rel(d.rootDir, path)
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
