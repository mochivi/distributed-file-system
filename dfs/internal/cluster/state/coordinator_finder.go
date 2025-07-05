package state

import (
	"sync"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/utils"
	"google.golang.org/grpc"
)

type CoordinatorFinder interface {
	GetCoordinator(nodeID string) (clients.ICoordinatorClient, bool)
	AddCoordinator(node *common.NodeInfo)
	RemoveCoordinator(nodeID string)
	ListCoordinators() []*common.NodeInfo
	GetLeaderCoordinator() (clients.ICoordinatorClient, bool)
	BootstrapCoordinator() error
}

type ClientConnectionFunc func(node *common.NodeInfo, opts ...grpc.DialOption) (clients.ICoordinatorClient, error)

// Coordinator node manager
type coordinatorFinder struct {
	nodes                map[string]*common.NodeInfo // coordinator nodes
	mu                   sync.RWMutex
	clientConnectionFunc ClientConnectionFunc
}

func NewCoordinatorFinder() *coordinatorFinder {
	return &coordinatorFinder{
		nodes:                make(map[string]*common.NodeInfo),
		mu:                   sync.RWMutex{},
		clientConnectionFunc: clients.NewCoordinatorClient, // for testing
	}
}

func (m *coordinatorFinder) BootstrapCoordinator() error {
	coordinatorHost := utils.GetEnvString("COORDINATOR_HOST", "coordinator")
	coordinatorPort := utils.GetEnvInt("COORDINATOR_PORT", 8080)
	coordinatorNode := &common.NodeInfo{
		ID:     "coordinator", // TODO: change to coordinator ID when implemented, should be received from some service discovery/config storage system
		Host:   coordinatorHost,
		Port:   coordinatorPort,
		Status: common.NodeHealthy,
	}
	m.AddCoordinator(coordinatorNode)
	return nil
}

func (m *coordinatorFinder) AddCoordinator(node *common.NodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.ID] = node
}

func (m *coordinatorFinder) RemoveCoordinator(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, nodeID)
}

// TODO: Should implement a leader election algorithm
func (m *coordinatorFinder) GetCoordinator(nodeID string) (clients.ICoordinatorClient, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.nodes) == 0 {
		return nil, false
	}

	node, ok := m.nodes[nodeID]
	if !ok {
		return nil, false
	}
	coordinatorClient, err := m.clientConnectionFunc(node)
	if err != nil {
		return nil, false
	}
	return coordinatorClient, true
}

func (m *coordinatorFinder) GetLeaderCoordinator() (clients.ICoordinatorClient, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.nodes) == 0 {
		return nil, false
	}

	// TODO: implement coordinator node selection algorithm
	for _, node := range m.nodes {
		if node.Status == common.NodeHealthy {
			coordinatorClient, err := m.clientConnectionFunc(node)
			if err != nil {
				// Couldn't connect, try the next one
				continue
			}
			return coordinatorClient, true
		}
	}

	return nil, false
}

func (m *coordinatorFinder) ListCoordinators() []*common.NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.nodes) == 0 {
		return nil
	}

	nodes := make([]*common.NodeInfo, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func (m *coordinatorFinder) SetClientConnectionFunc(clientConnectionFunc ClientConnectionFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clientConnectionFunc = clientConnectionFunc
}
