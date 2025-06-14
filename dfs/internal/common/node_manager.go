package common

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type NodeManager struct {
	nodes    map[string]*DataNodeInfo
	mu       sync.RWMutex
	selector NodeSelector

	// Version control fields
	currentVersion int64
	updateHistory  []NodeUpdate // Circular buffer for recent updates
	maxHistorySize int
	historyIndex   int // Current position in circular buffer
}

func NewNodeManager(selector NodeSelector) *NodeManager {
	return &NodeManager{
		nodes:          make(map[string]*DataNodeInfo),
		selector:       selector,
		currentVersion: 0,
		updateHistory:  make([]NodeUpdate, 1000), // Keep last 1000 node updates in stash
		maxHistorySize: 1000,
		historyIndex:   0,
	}
}

func (m *NodeManager) GetNode(nodeID string) (*DataNodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	node, ok := m.nodes[nodeID]
	if !ok {
		return nil, false
	}
	return node, true
}

func (m *NodeManager) AddNode(node *DataNodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.ID] = node
	m.addToHistory(NODE_ADDED, node)
}

func (m *NodeManager) RemoveNode(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, ok := m.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}
	delete(m.nodes, nodeID)
	m.addToHistory(NODE_REMOVED, node)
	return nil
}

func (m *NodeManager) UpdateNode(node *DataNodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, ok := m.nodes[node.ID]
	if !ok {
		return fmt.Errorf("node with ID %s not found", node.ID)
	}
	m.nodes[node.ID] = node
	m.addToHistory(NODE_UPDATED, node)
	return nil
}

func (m *NodeManager) ListNodes() ([]*DataNodeInfo, int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodes := make([]*DataNodeInfo, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}
	return nodes, m.currentVersion
}

// GetUpdatesSince returns all updates since the given version
func (m *NodeManager) GetUpdatesSince(sinceVersion int64) ([]NodeUpdate, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if requested version is too old (not in our history buffer)
	oldestVersion := m.getOldestVersionInHistory()
	if sinceVersion < oldestVersion {
		return nil, 0, fmt.Errorf("version %d too old, oldest available: %d", sinceVersion, oldestVersion)
	}

	// If already up to date
	if sinceVersion >= m.currentVersion {
		return []NodeUpdate{}, m.currentVersion, nil
	}

	// Collect updates since requested version
	var updates []NodeUpdate
	for i := 0; i < m.maxHistorySize; i++ {
		update := m.updateHistory[i]
		if update.Version > sinceVersion && update.Version <= m.currentVersion {
			updates = append(updates, update)
		}
	}

	return updates, m.currentVersion, nil
}

// getOldestVersionInHistory returns the oldest version still available in history
func (m *NodeManager) getOldestVersionInHistory() int64 {
	if m.currentVersion < int64(m.maxHistorySize) {
		return 0 // We have full history
	}
	return m.currentVersion - int64(m.maxHistorySize) + 1
}

// GetCurrentVersion returns the current version number
func (m *NodeManager) GetCurrentVersion() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentVersion
}

// IsVersionTooOld checks if a version is too old to serve incrementally
func (m *NodeManager) IsVersionTooOld(version int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return version < m.getOldestVersionInHistory()
}

// TODO: implement selection algorithm, right now, just picking the first healthy nodes
// Selects n nodes that could receive some chunk for storage
func (m *NodeManager) SelectBestNodes(n int, self ...string) ([]*DataNodeInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var selfID string
	if len(self) != 0 {
		selfID = self[0]
	}

	nodes := make([]*DataNodeInfo, 0, len(m.nodes))
	for _, node := range m.nodes {
		if node.ID == selfID {
			continue // do not select the own node making the request if being used for replication
		}
		nodes = append(nodes, node)
	}

	bestNodes := m.selector.SelectBestNodes(nodes, n)

	if len(bestNodes) == 0 {
		return nil, errors.New("no available nodes")
	}

	return bestNodes, nil
}

// Retrieves which nodes have some chunk
func (m *NodeManager) GetAvailableNodeForChunk(replicaIDs []*DataNodeInfo) (*DataNodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, replica := range replicaIDs {
		if node, exists := m.nodes[replica.ID]; exists && node.Status == NodeHealthy {
			return node, true
		}
	}

	return nil, false
}

// addToHistory adds an update to the circular buffer
func (m *NodeManager) addToHistory(updateType NodeUpdateType, node *DataNodeInfo) {
	m.currentVersion++

	update := NodeUpdate{
		Version:   m.currentVersion,
		Type:      updateType,
		Node:      node,
		Timestamp: time.Now(),
	}

	m.updateHistory[m.historyIndex] = update
	m.historyIndex = (m.historyIndex + 1) % m.maxHistorySize
}

// Given an array of NodeUpdate, apply changes to nodes in order
// Only used by data nodes
func (m *NodeManager) ApplyHistory(updates []NodeUpdate) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Apply updates in order
	for _, update := range updates {
		switch update.Type {
		case NODE_ADDED:
			m.nodes[update.Node.ID] = update.Node

		case NODE_REMOVED:
			delete(m.nodes, update.Node.ID)

		case NODE_UPDATED:
			// We should update regardless of whether it exists locally
			// to stay in sync with the master
			m.nodes[update.Node.ID] = update.Node
		}

		// Update our local version to match the update version
		// This ensures we stay in sync with the master's version
		if update.Version > m.currentVersion {
			m.currentVersion = update.Version
		}
	}
}

// Given an array of DataNodeInfo, initialize the no
// Only used by data nodes
func (m *NodeManager) InitializeNodes(nodes []*DataNodeInfo, currentVersion int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, node := range nodes {
		m.nodes[node.ID] = node
	}
	m.currentVersion = currentVersion
}
