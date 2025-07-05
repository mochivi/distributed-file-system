package shared

import (
	"context"
	"log/slog"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage"
)

type MetadataScannerProvider interface {
	// Used by the coordinator DeletedFilesGC
	// MetadataScannerProvider retrieves all files with delete flag = true
	// For each file, it will retrieve all chunk locations from the FileInfo struct which contains the chunk locations
	// With this information, the scanner returns a map of all chunkIDs and the nodes/replicas where they are stored
	// The GC takes over and coordinates the deletion of the chunks from the nodes/replicas
	// The MetadataScannerProvider maintains no state, it simply provides the information to the GC
	GetDeletedFiles(olderThan time.Time) ([]string, error)

	// Used by the datanode OrphanedChunksGC
	// MetadataScannerProvider retrieves all ChunkIDs that the datanode should be storing
	GetChunksForNode(nodeID string) (map[string]common.ChunkHeader, error)
}

type MetadataScannerService struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	store  storage.MetadataStore // Read-only access to the metadata store, retrieves files for scanning
	logger *slog.Logger
}

func NewMetadataScannerService(ctx context.Context, store storage.MetadataStore, logger *slog.Logger) *MetadataScannerService {
	ctx, cancel := context.WithCancelCause(ctx)

	return &MetadataScannerService{
		ctx:    ctx,
		cancel: cancel,
		store:  store,
		logger: logger,
	}
}

// 1. List all files in the metadata with the Deleted flag = true and older than the given time
// 2. Return the list of files
func (s *MetadataScannerService) GetDeletedFiles(olderThan time.Time) map[string][]string {
	return nil
}

// 1. List all chunks for the node making the request
// 2. Return the list of chunks
func (s *MetadataScannerService) GetChunksForNode(nodeID string) (map[string]common.ChunkHeader, error) {
	return nil, nil
}
