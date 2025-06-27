package node_manager

import (
	"fmt"
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
)

type datanodeManager struct {
	nodes      map[string]*common.DataNodeInfo // data nodes
	nodesMutex sync.RWMutex

	config DataNodeManagerConfig

	// Version control fields
	currentVersion int64
	updateHistory  []common.NodeUpdate // Circular buffer for recent updates
	historyIndex   int                 // Current position in circular buffer
}

func newDatanodeManager(config DataNodeManagerConfig) *datanodeManager {
	return &datanodeManager{
		nodes:          make(map[string]*common.DataNodeInfo),
		config:         config,
		currentVersion: 0,
		updateHistory:  make([]common.NodeUpdate, 1000), // Keep last 1000 node updates in stash
		historyIndex:   0,
	}
}

func (m *datanodeManager) getNode(nodeID string) (*common.DataNodeInfo, bool) {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()
	node, ok := m.nodes[nodeID]
	if !ok {
		return nil, false
	}
	return node, true
}

func (m *datanodeManager) addNode(node *common.DataNodeInfo) {
	m.nodesMutex.Lock()
	defer m.nodesMutex.Unlock()
	m.nodes[node.ID] = node
	m.addToHistory(common.NODE_ADDED, node)
}

func (m *datanodeManager) removeNode(nodeID string) error {
	m.nodesMutex.Lock()
	defer m.nodesMutex.Unlock()
	node, ok := m.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}
	delete(m.nodes, nodeID)
	m.addToHistory(common.NODE_REMOVED, node)
	return nil
}

func (m *datanodeManager) updateNode(node *common.DataNodeInfo) error {
	m.nodesMutex.Lock()
	defer m.nodesMutex.Unlock()
	node, ok := m.nodes[node.ID]
	if !ok {
		return fmt.Errorf("node with ID %s not found", node.ID)
	}
	m.nodes[node.ID] = node
	m.addToHistory(common.NODE_UPDATED, node)
	return nil
}

func (m *datanodeManager) listNodes(n ...int) ([]*common.DataNodeInfo, int64) {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()

	nodes := make([]*common.DataNodeInfo, 0, len(m.nodes))
	for _, node := range m.nodes {
		if len(n) > 0 && len(nodes) >= n[0] {
			break
		}
		nodes = append(nodes, node)
	}
	return nodes, m.currentVersion
}

// GetCurrentVersion returns the current version number
func (m *datanodeManager) getCurrentVersion() int64 {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()
	return m.currentVersion
}

// GetUpdatesSince returns all updates since the given version
func (m *datanodeManager) getUpdatesSince(sinceVersion int64) ([]common.NodeUpdate, int64, error) {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()

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
	for i := 0; i < m.config.MaxHistorySize; i++ {
		update := m.updateHistory[i]
		if update.Version > sinceVersion && update.Version <= m.currentVersion {
			updates = append(updates, update)
		}
	}

	return updates, m.currentVersion, nil
}

// IsVersionTooOld checks if a version is too old to serve incrementally
func (m *datanodeManager) isVersionTooOld(version int64) bool {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()
	return version < m.getOldestVersionInHistory()
}

// getOldestVersionInHistory returns the oldest version still available in history
func (m *datanodeManager) getOldestVersionInHistory() int64 {
	if m.currentVersion < int64(m.config.MaxHistorySize) {
		return 0 // We have full history
	}
	return m.currentVersion - int64(m.config.MaxHistorySize) + 1
}

// addToHistory adds an update to the circular buffer
func (m *datanodeManager) addToHistory(updateType common.NodeUpdateType, node *common.DataNodeInfo) {
	m.currentVersion++

	update := common.NodeUpdate{
		Version:   m.currentVersion,
		Type:      updateType,
		Node:      node,
		Timestamp: time.Now(),
	}

	m.updateHistory[m.historyIndex] = update
	m.historyIndex = (m.historyIndex + 1) % m.config.MaxHistorySize
}

// Given an array of NodeUpdate, apply changes to nodes in order
// Only used by data nodes when receiving updates from the coordinator
func (m *datanodeManager) applyHistory(updates []common.NodeUpdate) {
	m.nodesMutex.Lock()
	defer m.nodesMutex.Unlock()

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
// only used by data nodes when registering with the coordinator
func (m *datanodeManager) initializeNodes(nodes []*common.DataNodeInfo, currentVersion int64) {
	m.nodesMutex.Lock()
	defer m.nodesMutex.Unlock()
	m.nodes = make(map[string]*common.DataNodeInfo)
	for _, node := range nodes {
		m.nodes[node.ID] = node
	}
	m.currentVersion = currentVersion
}

// TODO: implement selection algorithm, right now, just picking the first healthy nodes
// Selects n nodes that could receive some chunk for storage
// func (m *nodeManager) selectBestNodes(n int, self ...string) ([]*common.DataNodeInfo, error) {
// 	m.nodesMutex.RLock()
// 	defer m.nodesMutex.RUnlock()

// 	var selfID string
// 	if len(self) != 0 {
// 		selfID = self[0]
// 	}

// 	nodes := make([]*common.DataNodeInfo, 0, len(m.nodes))
// 	for _, node := range m.nodes {
// 		if node.ID == selfID {
// 			continue // do not select the own node making the request if being used for replication
// 		}
// 		nodes = append(nodes, node)
// 	}

// 	bestNodes := m.selector.SelectBestNodes(nodes, n)

// 	if len(bestNodes) == 0 {
// 		return nil, errors.New("no available nodes")
// 	}

// 	return bestNodes, nil
// }

// // Retrieves which nodes have some chunk
// TODO: implement a check with the data nodes to see if they have the chunk before adding
func (m *datanodeManager) getAvailableNodesForChunk(replicaIDs []*common.DataNodeInfo) ([]*common.DataNodeInfo, bool) {
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
