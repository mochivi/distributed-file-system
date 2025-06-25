package cluster

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/utils"
)

type INodeManager interface {
	GetCurrentVersion() int64
	GetNode(nodeID string) (*common.DataNodeInfo, bool)
	AddNode(node *common.DataNodeInfo)
	RemoveNode(nodeID string) error
	UpdateNode(node *common.DataNodeInfo) error
	ListNodes() ([]*common.DataNodeInfo, int64)
	GetUpdatesSince(sinceVersion int64) ([]common.NodeUpdate, int64, error)
	IsVersionTooOld(version int64) bool
	SelectBestNodes(n int, self ...string) ([]*common.DataNodeInfo, error)
	GetAvailableNodesForChunk(replicaIDs []*common.DataNodeInfo) ([]*common.DataNodeInfo, bool)
	ApplyHistory(updates []common.NodeUpdate)
	InitializeNodes(nodes []*common.DataNodeInfo, currentVersion int64)
	GetCoordinatorNode() (*common.DataNodeInfo, bool)
	AddCoordinatorNode(node *common.DataNodeInfo)
	RemoveCoordinatorNode(nodeID string)
	ListCoordinatorNodes() ([]*common.DataNodeInfo, int64)
	BootstrapCoordinatorNode() error
}

type NodeManager struct {
	coordinatorNodes map[string]*common.DataNodeInfo // coordinator nodes
	nodes            map[string]*common.DataNodeInfo // data nodes
	mu               sync.RWMutex
	selector         common.NodeSelector

	// Version control fields
	currentVersion int64
	updateHistory  []common.NodeUpdate // Circular buffer for recent updates
	maxHistorySize int
	historyIndex   int // Current position in circular buffer
}

func NewNodeManager(selector common.NodeSelector) *NodeManager {
	return &NodeManager{
		coordinatorNodes: make(map[string]*common.DataNodeInfo),
		nodes:            make(map[string]*common.DataNodeInfo),
		selector:         selector,
		currentVersion:   0,
		updateHistory:    make([]common.NodeUpdate, 1000), // Keep last 1000 node updates in stash
		maxHistorySize:   1000,
		historyIndex:     0,
	}
}

func (m *NodeManager) BootstrapCoordinatorNode() error {
	coordinatorHost := utils.GetEnvString("COORDINATOR_HOST", "coordinator")
	coordinatorPort := utils.GetEnvInt("COORDINATOR_PORT", 8080)
	coordinatorNode := &common.DataNodeInfo{
		ID:     "coordinator", // TODO: change to coordinator ID when implemented, should be received from some service discovery/config storage system
		Host:   coordinatorHost,
		Port:   coordinatorPort,
		Status: common.NodeHealthy,
	}
	m.AddCoordinatorNode(coordinatorNode)
	return nil
}

func (m *NodeManager) AddCoordinatorNode(node *common.DataNodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.coordinatorNodes[node.ID] = node
}

func (m *NodeManager) RemoveCoordinatorNode(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.coordinatorNodes, nodeID)
}

func (m *NodeManager) GetCoordinatorNode() (*common.DataNodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.determineCoordinatorNode()
}

// TODO: implement coordinator node selection algorithm
func (m *NodeManager) determineCoordinatorNode() (*common.DataNodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.coordinatorNodes) == 0 {
		return nil, false
	}

	// TODO: implement coordinator node selection algorithm
	for _, node := range m.coordinatorNodes {
		if node.Status == common.NodeHealthy {
			return node, true
		}
	}

	return nil, false
}

func (m *NodeManager) ListCoordinatorNodes() ([]*common.DataNodeInfo, int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodes := make([]*common.DataNodeInfo, 0, len(m.coordinatorNodes))
	for _, node := range m.coordinatorNodes {
		nodes = append(nodes, node)
	}
	return nodes, m.currentVersion
}

func (m *NodeManager) GetNode(nodeID string) (*common.DataNodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	node, ok := m.nodes[nodeID]
	if !ok {
		return nil, false
	}
	return node, true
}

func (m *NodeManager) AddNode(node *common.DataNodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.ID] = node
	m.addToHistory(common.NODE_ADDED, node)
}

func (m *NodeManager) RemoveNode(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, ok := m.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}
	delete(m.nodes, nodeID)
	m.addToHistory(common.NODE_REMOVED, node)
	return nil
}

func (m *NodeManager) UpdateNode(node *common.DataNodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, ok := m.nodes[node.ID]
	if !ok {
		return fmt.Errorf("node with ID %s not found", node.ID)
	}
	m.nodes[node.ID] = node
	m.addToHistory(common.NODE_UPDATED, node)
	return nil
}

func (m *NodeManager) ListNodes() ([]*common.DataNodeInfo, int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodes := make([]*common.DataNodeInfo, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}
	return nodes, m.currentVersion
}

// GetUpdatesSince returns all updates since the given version
func (m *NodeManager) GetUpdatesSince(sinceVersion int64) ([]common.NodeUpdate, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if requested version is too old (not in our history buffer)
	oldestVersion := m.getOldestVersionInHistory()
	if sinceVersion < oldestVersion {
		return nil, 0, fmt.Errorf("version %d too old, oldest available: %d", sinceVersion, oldestVersion)
	}

	// If already up to date
	if sinceVersion >= m.currentVersion {
		return []common.NodeUpdate{}, m.currentVersion, nil
	}

	// Collect updates since requested version
	var updates []common.NodeUpdate
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
func (m *NodeManager) SelectBestNodes(n int, self ...string) ([]*common.DataNodeInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var selfID string
	if len(self) != 0 {
		selfID = self[0]
	}

	nodes := make([]*common.DataNodeInfo, 0, len(m.nodes))
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
// TODO: implement a check with the data nodes to see if they have the chunk before adding
func (m *NodeManager) GetAvailableNodesForChunk(replicaIDs []*common.DataNodeInfo) ([]*common.DataNodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var nodes []*common.DataNodeInfo
	for _, replicaNode := range replicaIDs {
		// Check if the node is still considered available
		node, ok := m.nodes[replicaNode.ID]
		if ok && node.Status == common.NodeHealthy {
			nodes = append(nodes, node)
		}
	}

	if len(nodes) == 0 {
		return nil, false
	}

	return nodes, true
}

// addToHistory adds an update to the circular buffer
func (m *NodeManager) addToHistory(updateType common.NodeUpdateType, node *common.DataNodeInfo) {
	m.currentVersion++

	update := common.NodeUpdate{
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
func (m *NodeManager) ApplyHistory(updates []common.NodeUpdate) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Apply updates in order
	for _, update := range updates {
		switch update.Type {
		case common.NODE_ADDED:
			m.nodes[update.Node.ID] = update.Node

		case common.NODE_REMOVED:
			delete(m.nodes, update.Node.ID)

		case common.NODE_UPDATED:
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

// Given an array of DataNodeInfo, initialize the nodes
// Only used by data nodes
func (m *NodeManager) InitializeNodes(nodes []*common.DataNodeInfo, currentVersion int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = make(map[string]*common.DataNodeInfo)
	for _, node := range nodes {
		m.nodes[node.ID] = node
	}
	m.currentVersion = currentVersion
}
