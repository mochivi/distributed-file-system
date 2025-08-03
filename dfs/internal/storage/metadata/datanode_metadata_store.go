package metadata

import (
	"context"
	"time"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
)

// DatanodeMetadataStore is a wrapper around the MetadataStore that provides a way to get the chunks for a node
// Currently, it makes a call to the coordinator to get the chunks for a node
type DatanodeMetadataStore struct {
	coordinator clients.ICoordinatorClient
}

func NewDatanodeMetadataStore(coordinator clients.ICoordinatorClient) *DatanodeMetadataStore {
	return &DatanodeMetadataStore{coordinator: coordinator}
}

func (m *DatanodeMetadataStore) PutFile(ctx context.Context, path string, info *common.FileInfo) error {
	return nil
}

func (m *DatanodeMetadataStore) GetFile(ctx context.Context, path string) (*common.FileInfo, error) {
	return nil, nil
}

func (m *DatanodeMetadataStore) DeleteFile(ctx context.Context, path string) error {
	return nil
}

func (m *DatanodeMetadataStore) ListFiles(ctx context.Context, directory string, recursive bool) ([]*common.FileInfo, error) {
	return nil, nil
}

func (m *DatanodeMetadataStore) GetChunksForNode(ctx context.Context, nodeID string) (map[string]*common.ChunkHeader, error) {
	return nil, nil
}

// Noop -> datanode does not need to retrieve the deleted files
// It is the coordinator's responsibility to retrieve the deleted files and send bulk delete requests to the datanodes
func (m *DatanodeMetadataStore) GetDeletedFiles(ctx context.Context, olderThan time.Time) ([]*common.FileInfo, error) {
	return nil, nil
}
