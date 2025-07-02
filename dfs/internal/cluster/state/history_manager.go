package state

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
)

type ClusterStateHistoryManager interface {
	ClusterStateViewer
	ClusterStateManager
	GetUpdatesSince(sinceVersion int64) ([]common.NodeUpdate, int64, error)
	IsVersionTooOld(version int64) bool
	GetOldestVersionInHistory() int64
	GetAvailableNodesForChunk(replicaIDs []*common.DataNodeInfo) ([]*common.DataNodeInfo, bool)
}

type ClusterStateHistoryManagerConfig struct {
	MaxHistorySize int
}

type clusterStateHistoryManager struct {
	store  *nodeStore
	mu     sync.RWMutex
	config ClusterStateHistoryManagerConfig

	version      int64
	history      []common.NodeUpdate // Circular buffer for recent updates
	historyIndex int                 // Current position in circular buffer
}

func NewClusterStateHistoryManager(config ClusterStateHistoryManagerConfig) *clusterStateHistoryManager {
	if config.MaxHistorySize <= 0 {
		config.MaxHistorySize = 100 // Default value
	}
	return &clusterStateHistoryManager{
		store:   newNodeStore(),
		config:  config,
		history: make([]common.NodeUpdate, config.MaxHistorySize), // The buffer has no need to extend capacity
	}
}

func (m *clusterStateHistoryManager) GetNode(nodeID string) (*common.DataNodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.store.getNode(nodeID)
}

func (m *clusterStateHistoryManager) AddNode(node *common.DataNodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.version++
	m.store.addNode(node)
	m.addToHistory(common.NODE_ADDED, node)
}

func (m *clusterStateHistoryManager) RemoveNode(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, ok := m.store.getNode(nodeID)
	if !ok {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}
	m.version++
	m.store.removeNode(nodeID)
	m.addToHistory(common.NODE_REMOVED, node)
	return nil
}

func (m *clusterStateHistoryManager) UpdateNode(node *common.DataNodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.store.getNode(node.ID)
	if !ok {
		return fmt.Errorf("node with ID %s not found", node.ID)
	}
	m.version++
	m.store.updateNode(node)
	m.addToHistory(common.NODE_UPDATED, node)
	return nil
}

func (m *clusterStateHistoryManager) ListNodes(n ...int) ([]*common.DataNodeInfo, int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]*common.DataNodeInfo, 0, len(m.store.nodes))
	for _, node := range m.store.listNodes(n...) {
		if len(n) > 0 && len(nodes) >= n[0] {
			break
		}
		nodes = append(nodes, node)
	}
	return nodes, m.version
}

// GetCurrentVersion returns the current version number
func (m *clusterStateHistoryManager) GetCurrentVersion() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.version
}

// GetUpdatesSince returns all updates since the given version
func (m *clusterStateHistoryManager) GetUpdatesSince(sinceVersion int64) ([]common.NodeUpdate, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if requested version is too old (not in our history buffer)
	oldestVersion := m.GetOldestVersionInHistory()
	if sinceVersion < oldestVersion {
		return nil, 0, fmt.Errorf("version %d too old, oldest available: %d", sinceVersion, oldestVersion)
	}

	// If already up to date
	if sinceVersion >= m.version {
		return []common.NodeUpdate{}, m.version, nil
	}

	var updates []common.NodeUpdate
	// If buffer isn't full, history is from index 0 to historyIndex-1
	if m.version < int64(m.config.MaxHistorySize) {
		for i := 0; i < m.historyIndex; i++ {
			update := m.history[i]
			if update.Version > sinceVersion {
				updates = append(updates, update)
			}
		}
	} else { // Buffer has wrapped around, so we need to check all of it
		for _, update := range m.history {
			if update.Version > sinceVersion {
				updates = append(updates, update)
			}
		}
	}

	// Sort updates by version to ensure chronological order
	sort.Slice(updates, func(i, j int) bool {
		return updates[i].Version < updates[j].Version
	})

	return updates, m.version, nil
}

// IsVersionTooOld checks if a version is too old to serve incrementally
func (m *clusterStateHistoryManager) IsVersionTooOld(version int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return version < m.GetOldestVersionInHistory()
}

// getOldestVersionInHistory returns the oldest version still available in history
func (m *clusterStateHistoryManager) GetOldestVersionInHistory() int64 {
	if m.version < int64(m.config.MaxHistorySize) {
		return 0 // We have full history
	}
	return m.version - int64(m.config.MaxHistorySize) + 1
}

// Given an array of NodeUpdate, apply changes to nodes in order
// Only used by data nodes when receiving updates from the coordinator
func (m *clusterStateHistoryManager) ApplyUpdates(updates []common.NodeUpdate) {
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
			// to stay in sync with the master
			m.store.updateNode(update.Node)
		}

		// Update our local version to match the update version
		// This ensures we stay in sync with the master's version
		if update.Version > m.version {
			m.version = update.Version
		}
	}
}

// Given an array of DataNodeInfo, initialize the nodes
// only used by data nodes when registering with the coordinator
func (m *clusterStateHistoryManager) InitializeNodes(nodes []*common.DataNodeInfo, currentVersion int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store.initializeNodes(nodes)
	m.version = currentVersion
}

func (m *clusterStateHistoryManager) GetAvailableNodesForChunk(replicaIDs []*common.DataNodeInfo) ([]*common.DataNodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var nodes []*common.DataNodeInfo
	for _, replicaNode := range replicaIDs {
		// Check if the node is still considered available
		node, ok := m.store.getNode(replicaNode.ID)
		if ok && node.Status == common.NodeHealthy {
			nodes = append(nodes, node)
		}
	}

	if len(nodes) == 0 {
		return nil, false
	}

	return nodes, true
}

func (m *clusterStateHistoryManager) addToHistory(updateType common.NodeUpdateType, node *common.DataNodeInfo) {
	update := common.NodeUpdate{
		Version:   m.version,
		Type:      updateType,
		Node:      node,
		Timestamp: time.Now(),
	}
	m.history[m.historyIndex] = update
	m.historyIndex = (m.historyIndex + 1) % m.config.MaxHistorySize
}
