package state

import (
	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
)

type MockClusterStateManager struct {
	mock.Mock
}

func (m *MockClusterStateManager) GetNode(nodeID string) (*common.NodeInfo, error) {
	args := m.Called(nodeID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.NodeInfo), args.Error(1)
}

func (m *MockClusterStateManager) ListNodes(n ...int) ([]*common.NodeInfo, int64) {
	args := m.Called(n)
	return args.Get(0).([]*common.NodeInfo), args.Get(1).(int64)
}

func (m *MockClusterStateManager) AddNode(node *common.NodeInfo) {
	m.Called(node)
}

func (m *MockClusterStateManager) RemoveNode(nodeID string) error {
	args := m.Called(nodeID)
	return args.Error(0)
}

func (m *MockClusterStateManager) UpdateNode(node *common.NodeInfo) error {
	args := m.Called(node)
	return args.Error(0)
}

func (m *MockClusterStateManager) ApplyUpdates(updates []common.NodeUpdate) {
	m.Called(updates)
}

func (m *MockClusterStateManager) InitializeNodes(nodes []*common.NodeInfo, version int64) {
	m.Called(nodes, version)
}

func (m *MockClusterStateManager) GetCurrentVersion() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

type MockClusterStateHistoryManager struct {
	mock.Mock
}

func (m *MockClusterStateHistoryManager) ListNodes(n ...int) ([]*common.NodeInfo, int64) {
	args := m.Called(n)
	if args.Get(0) == nil {
		return nil, args.Get(1).(int64)
	}
	return args.Get(0).([]*common.NodeInfo), args.Get(1).(int64)
}

func (m *MockClusterStateHistoryManager) GetNode(nodeID string) (*common.NodeInfo, error) {
	args := m.Called(nodeID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.NodeInfo), args.Error(1)
}

func (m *MockClusterStateHistoryManager) AddNode(node *common.NodeInfo) {
	m.Called(node)
}

func (m *MockClusterStateHistoryManager) RemoveNode(nodeID string) error {
	args := m.Called(nodeID)
	return args.Error(0)
}

func (m *MockClusterStateHistoryManager) UpdateNode(node *common.NodeInfo) error {
	args := m.Called(node)
	return args.Error(0)
}

func (m *MockClusterStateHistoryManager) InitializeNodes(nodes []*common.NodeInfo, version int64) {
	m.Called(nodes, version)
}

func (m *MockClusterStateHistoryManager) ApplyUpdates(updates []common.NodeUpdate) {
	m.Called(updates)
}

func (m *MockClusterStateHistoryManager) GetCurrentVersion() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

func (m *MockClusterStateHistoryManager) GetUpdatesSince(sinceVersion int64) ([]common.NodeUpdate, int64, error) {
	args := m.Called(sinceVersion)
	if args.Get(0) == nil {
		return nil, args.Get(1).(int64), args.Error(2)
	}
	return args.Get(0).([]common.NodeUpdate), args.Get(1).(int64), args.Error(2)
}

func (m *MockClusterStateHistoryManager) IsVersionTooOld(version int64) bool {
	args := m.Called(version)
	return args.Bool(0)
}

func (m *MockClusterStateHistoryManager) GetOldestVersionInHistory() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

func (m *MockClusterStateHistoryManager) GetAvailableNodesForChunk(replicaIDs []*common.NodeInfo) ([]*common.NodeInfo, bool) {
	args := m.Called(replicaIDs)
	if args.Get(0) == nil {
		return nil, args.Bool(1)
	}
	return args.Get(0).([]*common.NodeInfo), args.Bool(1)
}

type MockCoordinatorFinder struct {
	mock.Mock
}

func (m *MockCoordinatorFinder) GetCoordinator(nodeID string) (clients.ICoordinatorClient, bool) {
	args := m.Called(nodeID)
	if args.Get(0) == nil {
		return nil, args.Bool(1)
	}
	return args.Get(0).(clients.ICoordinatorClient), args.Bool(1)
}

func (m *MockCoordinatorFinder) AddCoordinator(node *common.NodeInfo) {
	m.Called(node)
}

func (m *MockCoordinatorFinder) RemoveCoordinator(nodeID string) {
	m.Called(nodeID)
}

func (m *MockCoordinatorFinder) ListCoordinators() []*common.NodeInfo {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]*common.NodeInfo)
}

func (m *MockCoordinatorFinder) GetLeaderCoordinator() (clients.ICoordinatorClient, bool) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Bool(1)
	}
	return args.Get(0).(clients.ICoordinatorClient), args.Bool(1)
}

func (m *MockCoordinatorFinder) BootstrapCoordinator() error {
	args := m.Called()
	return args.Error(0)
}
