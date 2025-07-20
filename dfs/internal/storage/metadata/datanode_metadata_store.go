package metadata

import (
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

func (m *DatanodeMetadataStore) PutFile(path string, info *common.FileInfo) error {
	return nil
}

func (m *DatanodeMetadataStore) GetFile(path string) (*common.FileInfo, error) {
	return nil, nil
}

func (m *DatanodeMetadataStore) DeleteFile(path string) error {
	return nil
}

func (m *DatanodeMetadataStore) ListFiles(directory string, recursive bool) ([]*common.FileInfo, error) {
	return nil, nil
}

func (m *DatanodeMetadataStore) GetChunksForNode(nodeID string) (map[string]*common.ChunkHeader, error) {
	return nil, nil
}

// Noop -> datanode does not need to retrieve the deleted files
// It is the coordinator's responsibility to retrieve the deleted files and send bulk delete requests to the datanodes
func (m *DatanodeMetadataStore) GetDeletedFiles(olderThan time.Time) ([]*common.FileInfo, error) {
	return nil, nil
}
