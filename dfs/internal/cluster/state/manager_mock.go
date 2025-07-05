package state

import (
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
)

type MockClusterStateManager struct {
	mock.Mock
}

func NewMockClusterStateManager() *MockClusterStateManager {
	return &MockClusterStateManager{
		Mock: mock.Mock{},
	}
}

func (m *MockClusterStateManager) GetNode(nodeID string) (*common.NodeInfo, bool) {
	args := m.Called(nodeID)
	return args.Get(0).(*common.NodeInfo), args.Get(1).(bool)
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
