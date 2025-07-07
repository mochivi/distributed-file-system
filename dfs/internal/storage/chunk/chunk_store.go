package chunk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage/encoding"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

type ChunkDiskStorage struct {
	config     config.DiskStorageConfig
	logger     *slog.Logger
	serializer encoding.ChunkSerializer
}

func NewChunkDiskStorage(config config.DiskStorageConfig, serializer encoding.ChunkSerializer, logger *slog.Logger) (*ChunkDiskStorage, error) {
	storageLogger := logging.ExtendLogger(logger, slog.String("component", "chunk_storage"))
	if err := os.MkdirAll(config.RootDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create rootDir for chunk disk storage: %w", err)
	}
	storageLogger.Info("Created chunk disk storage rootDir", slog.String("root_dir", config.RootDir))
	return &ChunkDiskStorage{
		config:     config,
		logger:     storageLogger,
		serializer: serializer,
	}, nil
}

func (d *ChunkDiskStorage) Store(chunkHeader common.ChunkHeader, data []byte) error {
	fullPath, err := d.getChunkPath(chunkHeader.ID)
	if err != nil {
		return err
	}

	// Serialize chunkHeader
	serializedHeader, err := d.serializer.SerializeHeader(chunkHeader)
	if err != nil {
		return fmt.Errorf("failed to serialize chunk: %w", err)
	}
	data = append(serializedHeader, data...) // TODO: performance can be better by reserving the space in the file for the header and data, then write separately

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

	d.logger.Info("Stored chunk", slog.String("chunk_id", chunkHeader.ID), slog.String("path", fullPath))
	return nil
}

// Get returns the data of a chunk with the header.
func (d *ChunkDiskStorage) Get(chunkID string) (common.ChunkHeader, []byte, error) {
	fullPath, err := d.getChunkPath(chunkID)
	if err != nil {
		return common.ChunkHeader{}, nil, err
	}

	header, err := d.GetHeader(chunkID)
	if err != nil {
		return common.ChunkHeader{}, nil, fmt.Errorf("failed to get chunk header: %w", err)
	}

	data, err := d.GetData(chunkID)
	if err != nil {
		return common.ChunkHeader{}, nil, fmt.Errorf("failed to get chunk data: %w", err)
	}

	d.logger.Info("Retrieved chunk", slog.String("chunk_id", chunkID), slog.String("path", fullPath))
	return header, data, nil
}

// GetData returns the data of a chunk without the header.
func (d *ChunkDiskStorage) GetData(chunkID string) ([]byte, error) {
	fullPath, err := d.getChunkPath(chunkID)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk file %s: %w", fullPath, err)
	}

	// Consume the header with the same logic used when deserialising.
	if _, err := d.serializer.DeserializeHeader(file); err != nil {
		return nil, err
	}

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk data: %w", err)
	}

	d.logger.Info("Retrieved chunk data", slog.String("chunk_id", chunkID), slog.String("path", fullPath))
	return data, nil
}

// GetHeader returns the header of a chunk.
func (d *ChunkDiskStorage) GetHeader(chunkID string) (common.ChunkHeader, error) {
	fullPath, err := d.getChunkPath(chunkID)
	if err != nil {
		return common.ChunkHeader{}, err
	}

	file, err := os.Open(fullPath)
	if err != nil {
		return common.ChunkHeader{}, fmt.Errorf("failed to read chunk file %s: %w", fullPath, err)
	}
	defer file.Close()

	header, err := d.serializer.DeserializeHeader(file)
	if err != nil {
		return common.ChunkHeader{}, fmt.Errorf("failed to deserialize chunk header: %w", err)
	}
	return header, nil
}

// Reads all headers for provided chunkIDs, maps them
// Returns all headers if chunkIDs is not provided
func (d *ChunkDiskStorage) GetHeaders(ctx context.Context) (map[string]common.ChunkHeader, error) {
	maxWorkers := 10

	// Queue work into channel
	paths := make([]string, maxWorkers)
	if err := filepath.Walk(d.config.RootDir, func(path string, info fs.FileInfo, err error) error {
		paths = append(paths, path)
		return nil
	}); err != nil {
		return nil, err
	}

	sem := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup

	// Collect results
	type result struct {
		header common.ChunkHeader
		err    error
	}
	resultCh := make(chan result, maxWorkers)

	// Read chunk headers
	for _, path := range paths {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				resultCh <- result{err: ctx.Err()}
				return
			case sem <- struct{}{}:
				defer func() { <-sem }()
			}

			file, err := os.Open(p)
			if err != nil {
				resultCh <- result{err: fmt.Errorf("failed to read chunk header %s: %w", p, err)}
				return
			}
			defer file.Close()

			header, err := d.serializer.DeserializeHeader(file)
			if err != nil {
				resultCh <- result{err: fmt.Errorf("failed to deserialize chunk header: %w", err)}
				return
			}

			resultCh <- result{header: header}
		}(path)
	}
	wg.Wait()
	close(resultCh)

	// Inneficient, would be best to know how many files are stored first and adding capacity
	headers := make(map[string]common.ChunkHeader, 0)
outer:
	for {
		select {
		case res, ok := <-resultCh:
			if !ok { // channel closed and emptied out
				break outer
			}
			if res.err != nil {
				continue // No action on reading errors for now
			}
			// This should be fine as there is only one file per chunk ID, so no overwrites
			headers[res.header.ID] = res.header
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return headers, nil
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
			return err
		}
		if !empty {
			break
		}
		if err := os.Remove(dirPath); err != nil {
			return err
		}
		dirPath = filepath.Dir(dirPath)
	}

	d.logger.Info("Deleted chunk", slog.String("chunk_id", chunkID), slog.String("path", fullPath))
	return nil
}

// Important considerations
//  1. Context cancellation should return an error on the worker for the chunkID
//  2. Deletion failures should be aggregated and returned
//
// TODO: maybe move into a worker pool design, if too many chunks are attemped to be deleted
// TODO: it could cause a lot of goroutines to just stand around
func (d *ChunkDiskStorage) BulkDelete(ctx context.Context, maxConcurrentDeletes int, chunkIDs []string) ([]string, error) {
	if len(chunkIDs) == 0 {
		return nil, nil
	}

	sem := make(chan struct{}, maxConcurrentDeletes)
	var wg sync.WaitGroup

	type result struct {
		id  string
		err error
	}
	resultCh := make(chan result, maxConcurrentDeletes)

	// Launch all goroutines - not the best
	for _, chunkID := range chunkIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()

			// Try to acquire lock or return on context cancellation
			select {
			case <-ctx.Done(): // Report any chunks left over after context cancellation
				resultCh <- result{id: id, err: ctx.Err()}
				return
			case sem <- struct{}{}:
				defer func() { <-sem }()
			}

			// Re-check context in case it was cancelled while waiting for the semaphore
			if ctx.Err() != nil {
				resultCh <- result{id: id, err: ctx.Err()}
				return
			}

			// Delete chunk
			err := d.Delete(id)
			resultCh <- result{id: id, err: err}
		}(chunkID)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Process results, use sets for faster lookups
	// We do not check the context here, as we should return the list of failed/pending chunks
	failed := make(map[string]bool, 0)
	deleted := make(map[string]bool, 0)
	for res := range resultCh {
		if res.err != nil {
			failed[res.id] = true
		} else {
			deleted[res.id] = true
		}
	}

	// Success: all chunks were deleted
	if len(deleted) == len(chunkIDs) {
		return nil, nil
	}

	// Sanity check of results
	for _, id := range chunkIDs {
		if !failed[id] && !deleted[id] {
			failed[id] = true
		}
	}

	// Convert failed map to slice for return
	failedSlice := make([]string, 0, len(failed))
	for id := range failed {
		failedSlice = append(failedSlice, id)
	}

	return failedSlice, fmt.Errorf("failed to delete %d out of %d chunks", len(failed), len(chunkIDs))
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

// Helpers

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
