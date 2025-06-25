package cluster

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/utils"
)

// INodeManager defines the interface for managing the cluster's node information from the
// perspective of a single node. It is designed to be used with dependency injection,
// allowing for flexible implementations, such as mocks for testing, which facilitates
// decoupled and testable components, for example, a mock could enforce writing to a specific node.
//
// An implementation of INodeManager, such as NodeManager, is a core component within a
// DataNode server. Its responsibilities include:
//
//  1. Node Registry: Maintaining a synchronized list of all known data nodes and
//     coordinator nodes in the cluster.
//
//  2. State Synchronization: Coordinating with the master Coordinator to keep the local node
//     list up-to-date. This is achieved through a versioning system (e.g., GetUpdatesSince,
//     ApplyHistory) that allows for efficient, incremental updates rather than fetching the
//     entire node list repeatedly.
//
//  3. Coordinator Leader Discovery: Identifying and providing access to the current leader of
//     the coordinator cluster (e.g., GetCoordinatorNode). This is crucial for DataNodes, which
//     send write requests like heartbeats and block reports to the leader. Caching the leader's
//     identity within the NodeManager makes these interactions more efficient.
//
//  4. Node Selection: Providing logic to select appropriate nodes for various tasks, such as
//     selecting datanodes for chunk replication (e.g., SelectBestNodes).
type INodeManager interface {
	GetCurrentVersion() int64
	GetNode(nodeID string) (*common.DataNodeInfo, bool)
	AddNode(node *common.DataNodeInfo)
	RemoveNode(nodeID string) error
	UpdateNode(node *common.DataNodeInfo) error
	ListNodes() ([]*common.DataNodeInfo, int64)
	GetUpdatesSince(sinceVersion int64) ([]NodeUpdate, int64, error)
	IsVersionTooOld(version int64) bool
	SelectBestNodes(n int, self ...string) ([]*common.DataNodeInfo, error)
	GetAvailableNodesForChunk(replicaIDs []*common.DataNodeInfo) ([]*common.DataNodeInfo, bool)
	ApplyHistory(updates []NodeUpdate)
	InitializeNodes(nodes []*common.DataNodeInfo, currentVersion int64)
	GetCoordinatorNode() (*common.DataNodeInfo, bool)
	AddCoordinatorNode(node *common.DataNodeInfo)
	RemoveCoordinatorNode(nodeID string)
	ListCoordinatorNodes() ([]*common.DataNodeInfo, int64)
	BootstrapCoordinatorNode() error
}

type NodeManagerConfig struct {
	MaxHistorySize int // max number of updates to keep in history
}

func DefaultNodeManagerConfig() NodeManagerConfig {
	return NodeManagerConfig{
		MaxHistorySize: 1000,
	}
}

type NodeManager struct {
	coordinatorNodes map[string]*common.DataNodeInfo // coordinator nodes
	nodes            map[string]*common.DataNodeInfo // data nodes
	nodesMutex       sync.RWMutex
	selector         common.NodeSelector

	NodeManagerConfig NodeManagerConfig

	// Version control fields
	currentVersion int64
	updateHistory  []NodeUpdate // Circular buffer for recent updates
	historyIndex   int          // Current position in circular buffer
}

func NewNodeManager(selector common.NodeSelector, config NodeManagerConfig) *NodeManager {
	return &NodeManager{
		coordinatorNodes:  make(map[string]*common.DataNodeInfo),
		nodes:             make(map[string]*common.DataNodeInfo),
		selector:          selector,
		NodeManagerConfig: config,
		currentVersion:    0,
		updateHistory:     make([]NodeUpdate, 1000), // Keep last 1000 node updates in stash
		historyIndex:      0,
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
	m.nodesMutex.Lock()
	defer m.nodesMutex.Unlock()
	m.coordinatorNodes[node.ID] = node
}

func (m *NodeManager) RemoveCoordinatorNode(nodeID string) {
	m.nodesMutex.Lock()
	defer m.nodesMutex.Unlock()
	delete(m.coordinatorNodes, nodeID)
}

// TODO: Should implement a leader election algorithm
func (m *NodeManager) GetCoordinatorNode() (*common.DataNodeInfo, bool) {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()
	return m.determineCoordinatorNode()
}

// TODO: implement coordinator node selection algorithm
func (m *NodeManager) determineCoordinatorNode() (*common.DataNodeInfo, bool) {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()

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
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()
	nodes := make([]*common.DataNodeInfo, 0, len(m.coordinatorNodes))
	for _, node := range m.coordinatorNodes {
		nodes = append(nodes, node)
	}
	return nodes, m.currentVersion
}

func (m *NodeManager) GetNode(nodeID string) (*common.DataNodeInfo, bool) {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()
	node, ok := m.nodes[nodeID]
	if !ok {
		return nil, false
	}
	return node, true
}

func (m *NodeManager) AddNode(node *common.DataNodeInfo) {
	m.nodesMutex.Lock()
	defer m.nodesMutex.Unlock()
	m.nodes[node.ID] = node
	m.addToHistory(NODE_ADDED, node)
}

func (m *NodeManager) RemoveNode(nodeID string) error {
	m.nodesMutex.Lock()
	defer m.nodesMutex.Unlock()
	node, ok := m.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}
	delete(m.nodes, nodeID)
	m.addToHistory(NODE_REMOVED, node)
	return nil
}

func (m *NodeManager) UpdateNode(node *common.DataNodeInfo) error {
	m.nodesMutex.Lock()
	defer m.nodesMutex.Unlock()
	node, ok := m.nodes[node.ID]
	if !ok {
		return fmt.Errorf("node with ID %s not found", node.ID)
	}
	m.nodes[node.ID] = node
	m.addToHistory(NODE_UPDATED, node)
	return nil
}

func (m *NodeManager) ListNodes() ([]*common.DataNodeInfo, int64) {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()
	nodes := make([]*common.DataNodeInfo, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}
	return nodes, m.currentVersion
}

// GetUpdatesSince returns all updates since the given version
func (m *NodeManager) GetUpdatesSince(sinceVersion int64) ([]NodeUpdate, int64, error) {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()

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
	for i := 0; i < m.NodeManagerConfig.MaxHistorySize; i++ {
		update := m.updateHistory[i]
		if update.Version > sinceVersion && update.Version <= m.currentVersion {
			updates = append(updates, update)
		}
	}

	return updates, m.currentVersion, nil
}

// getOldestVersionInHistory returns the oldest version still available in history
func (m *NodeManager) getOldestVersionInHistory() int64 {
	if m.currentVersion < int64(m.NodeManagerConfig.MaxHistorySize) {
		return 0 // We have full history
	}
	return m.currentVersion - int64(m.NodeManagerConfig.MaxHistorySize) + 1
}

// GetCurrentVersion returns the current version number
func (m *NodeManager) GetCurrentVersion() int64 {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()
	return m.currentVersion
}

// IsVersionTooOld checks if a version is too old to serve incrementally
func (m *NodeManager) IsVersionTooOld(version int64) bool {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()
	return version < m.getOldestVersionInHistory()
}

// TODO: implement selection algorithm, right now, just picking the first healthy nodes
// Selects n nodes that could receive some chunk for storage
func (m *NodeManager) SelectBestNodes(n int, self ...string) ([]*common.DataNodeInfo, error) {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()

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
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()

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
func (m *NodeManager) addToHistory(updateType NodeUpdateType, node *common.DataNodeInfo) {
	m.currentVersion++

	update := NodeUpdate{
		Version:   m.currentVersion,
		Type:      updateType,
		Node:      node,
		Timestamp: time.Now(),
	}

	m.updateHistory[m.historyIndex] = update
	m.historyIndex = (m.historyIndex + 1) % m.NodeManagerConfig.MaxHistorySize
}

// Given an array of NodeUpdate, apply changes to nodes in order
// Only used by data nodes
func (m *NodeManager) ApplyHistory(updates []NodeUpdate) {
	m.nodesMutex.Lock()
	defer m.nodesMutex.Unlock()

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

// Given an array of DataNodeInfo, initialize the nodes
// Only used by data nodes
func (m *NodeManager) InitializeNodes(nodes []*common.DataNodeInfo, currentVersion int64) {
	m.nodesMutex.Lock()
	defer m.nodesMutex.Unlock()
	m.nodes = make(map[string]*common.DataNodeInfo)
	for _, node := range nodes {
		m.nodes[node.ID] = node
	}
	m.currentVersion = currentVersion
}
