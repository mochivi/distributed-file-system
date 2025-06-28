package state

import (
	"sync"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/utils"
)

type CoordinatorFinder interface {
	GetCoordinatorNode(nodeID string) (*common.DataNodeInfo, bool)
	AddCoordinatorNode(node *common.DataNodeInfo)
	RemoveCoordinatorNode(nodeID string)
	ListCoordinatorNodes() []*common.DataNodeInfo
	GetLeaderCoordinatorNode() (*common.DataNodeInfo, bool)
	BootstrapCoordinatorNode() error
}

// Coordinator node manager
type coordinatorFinder struct {
	nodes map[string]*common.DataNodeInfo // coordinator nodes
	mu    sync.RWMutex
}

func NewCoordinatorFinder() *coordinatorFinder {
	return &coordinatorFinder{
		nodes: make(map[string]*common.DataNodeInfo),
		mu:    sync.RWMutex{},
	}
}

func (m *coordinatorFinder) BootstrapCoordinatorNode() error {
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

func (m *coordinatorFinder) AddCoordinatorNode(node *common.DataNodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.ID] = node
}

func (m *coordinatorFinder) RemoveCoordinatorNode(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, nodeID)
}

// TODO: Should implement a leader election algorithm
func (m *coordinatorFinder) GetCoordinatorNode(nodeID string) (*common.DataNodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.nodes) == 0 {
		return nil, false
	}

	node, ok := m.nodes[nodeID]
	return node, ok
}

func (m *coordinatorFinder) GetLeaderCoordinatorNode() (*common.DataNodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.nodes) == 0 {
		return nil, false
	}

	// TODO: implement coordinator node selection algorithm
	for _, node := range m.nodes {
		if node.Status == common.NodeHealthy {
			return node, true
		}
	}

	return nil, false
}

func (m *coordinatorFinder) ListCoordinatorNodes() []*common.DataNodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.nodes) == 0 {
		return nil
	}

	nodes := make([]*common.DataNodeInfo, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}
