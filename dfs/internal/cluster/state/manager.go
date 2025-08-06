package state

import (
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
)

// Read-only interface
type ClusterStateViewer interface {
	ListNodes(n ...int) ([]*common.NodeInfo, int64)
	GetNode(nodeID string) (*common.NodeInfo, error)
}

// Complete interface
type ClusterStateManager interface {
	ClusterStateViewer
	AddNode(node *common.NodeInfo)
	RemoveNode(nodeID string) error
	UpdateNode(node *common.NodeInfo) error
	InitializeNodes(nodes []*common.NodeInfo, version int64)
	ApplyUpdates(updates []common.NodeUpdate)
	GetCurrentVersion() int64
}

// Implements ClusterStateViewer and ClusterStateManager
// Struct is shared with datanode agent and grpc server
type clusterStateManager struct {
	store   *nodeStore
	mu      sync.RWMutex
	version int64
}

func NewClusterStateManager() *clusterStateManager {
	return &clusterStateManager{
		store: newNodeStore(),
	}
}

func (v *clusterStateManager) GetNode(nodeID string) (*common.NodeInfo, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.store.getNode(nodeID)
}

func (v *clusterStateManager) ListNodes(n ...int) ([]*common.NodeInfo, int64) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.store.listNodes(n...), v.version
}

func (m *clusterStateManager) AddNode(node *common.NodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	node.LastSeen = time.Now()
	m.store.addNode(node)
	m.version++
}

func (m *clusterStateManager) RemoveNode(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.store.removeNode(nodeID); err != nil {
		return err
	}
	m.version++
	return nil
}

func (m *clusterStateManager) UpdateNode(node *common.NodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.store.updateNode(node); err != nil {
		return err
	}
	m.version++
	return nil
}

func (m *clusterStateManager) ApplyUpdates(updates []common.NodeUpdate) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Apply updates in order
	for _, update := range updates {
		switch update.Type {
		case common.NODE_ADDED:
			m.store.addNode(update.Node)

		case common.NODE_REMOVED:
			m.store.removeNode(update.Node.ID)

		case common.NODE_UPDATED:
			// We should update regardless of whether it exists locally
			// to stay in sync with the coordinator
			m.store.updateNode(update.Node)
		}

		// Update our local version to match the update version
		// This ensures we stay in sync with the coordinator's version
		if update.Version > m.version {
			m.version = update.Version
		}
	}
}

func (m *clusterStateManager) InitializeNodes(nodes []*common.NodeInfo, version int64) {
	m.store.initializeNodes(nodes)
	m.version = version
}

func (m *clusterStateManager) GetCurrentVersion() int64 {
	return m.version
}
