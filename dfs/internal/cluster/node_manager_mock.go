package cluster

import (
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
)

// MockNodeManager is a mock implementation of the NodeManager interface
type MockNodeManager struct {
	mock.Mock
}

func (m *MockNodeManager) BootstrapCoordinatorNode() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockNodeManager) GetCoordinatorNode() (*common.DataNodeInfo, bool) {
	args := m.Called()
	return args.Get(0).(*common.DataNodeInfo), args.Bool(1)
}

func (m *MockNodeManager) AddCoordinatorNode(node *common.DataNodeInfo) {
	m.Called(node)
}

func (m *MockNodeManager) RemoveCoordinatorNode(nodeID string) {
	m.Called(nodeID)
}

func (m *MockNodeManager) ListCoordinatorNodes() ([]*common.DataNodeInfo, int64) {
	args := m.Called()
	return args.Get(0).([]*common.DataNodeInfo), args.Get(1).(int64)
}

func (m *MockNodeManager) GetCurrentVersion() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

func (m *MockNodeManager) GetNode(nodeID string) (*common.DataNodeInfo, bool) {
	args := m.Called(nodeID)
	return args.Get(0).(*common.DataNodeInfo), args.Bool(1)
}

func (m *MockNodeManager) AddNode(node *common.DataNodeInfo) {
	m.Called(node)
}

func (m *MockNodeManager) RemoveNode(nodeID string) error {
	args := m.Called(nodeID)
	return args.Error(0)
}

func (m *MockNodeManager) UpdateNode(node *common.DataNodeInfo) error {
	args := m.Called(node)
	return args.Error(0)
}

func (m *MockNodeManager) ListNodes() ([]*common.DataNodeInfo, int64) {
	args := m.Called()
	return args.Get(0).([]*common.DataNodeInfo), args.Get(1).(int64)
}

func (m *MockNodeManager) GetUpdatesSince(sinceVersion int64) ([]common.NodeUpdate, int64, error) {
	args := m.Called(sinceVersion)
	return args.Get(0).([]common.NodeUpdate), args.Get(1).(int64), args.Error(2)
}

func (m *MockNodeManager) IsVersionTooOld(version int64) bool {
	args := m.Called(version)
	return args.Bool(0)
}

func (m *MockNodeManager) SelectBestNodes(n int, self ...string) ([]*common.DataNodeInfo, error) {
	args := m.Called(n, self)
	return args.Get(0).([]*common.DataNodeInfo), args.Error(1)
}

func (m *MockNodeManager) GetAvailableNodesForChunk(replicaIDs []*common.DataNodeInfo) ([]*common.DataNodeInfo, bool) {
	args := m.Called(replicaIDs)
	return args.Get(0).([]*common.DataNodeInfo), args.Bool(1)
}

func (m *MockNodeManager) ApplyHistory(updates []common.NodeUpdate) {
	m.Called(updates)
}

func (m *MockNodeManager) InitializeNodes(nodes []*common.DataNodeInfo, currentVersion int64) {
	m.Called(nodes, currentVersion)
}
