package node_manager

import (
	"sync"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/utils"
)

// Coordinator node manager
type coordinatorNodeManager struct {
	coordinatorNodes map[string]*common.DataNodeInfo // coordinator nodes
	nodesMutex       sync.RWMutex

	config CoordinatorNodeManagerConfig
}

func newCoordinatorNodeManager(config CoordinatorNodeManagerConfig) *coordinatorNodeManager {
	return &coordinatorNodeManager{
		coordinatorNodes: make(map[string]*common.DataNodeInfo),
		nodesMutex:       sync.RWMutex{},
		config:           config,
	}
}

func (m *coordinatorNodeManager) bootstrapCoordinatorNode() error {
	coordinatorHost := utils.GetEnvString("COORDINATOR_HOST", "coordinator")
	coordinatorPort := utils.GetEnvInt("COORDINATOR_PORT", 8080)
	coordinatorNode := &common.DataNodeInfo{
		ID:     "coordinator", // TODO: change to coordinator ID when implemented, should be received from some service discovery/config storage system
		Host:   coordinatorHost,
		Port:   coordinatorPort,
		Status: common.NodeHealthy,
	}
	m.addCoordinatorNode(coordinatorNode)
	return nil
}

func (m *coordinatorNodeManager) addCoordinatorNode(node *common.DataNodeInfo) {
	m.nodesMutex.Lock()
	defer m.nodesMutex.Unlock()
	m.coordinatorNodes[node.ID] = node
}

func (m *coordinatorNodeManager) removeCoordinatorNode(nodeID string) {
	m.nodesMutex.Lock()
	defer m.nodesMutex.Unlock()
	delete(m.coordinatorNodes, nodeID)
}

// TODO: Should implement a leader election algorithm
func (m *coordinatorNodeManager) getCoordinatorNode(nodeID string) (*common.DataNodeInfo, bool) {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()

	if len(m.coordinatorNodes) == 0 {
		return nil, false
	}

	node, ok := m.coordinatorNodes[nodeID]
	return node, ok
}

func (m *coordinatorNodeManager) getLeaderCoordinatorNode() (*common.DataNodeInfo, bool) {
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

func (m *coordinatorNodeManager) listCoordinatorNodes() []*common.DataNodeInfo {
	m.nodesMutex.RLock()
	defer m.nodesMutex.RUnlock()

	if len(m.coordinatorNodes) == 0 {
		return nil
	}

	nodes := make([]*common.DataNodeInfo, 0, len(m.coordinatorNodes))
	for _, node := range m.coordinatorNodes {
		nodes = append(nodes, node)
	}
	return nodes
}
