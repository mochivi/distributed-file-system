package common

import "github.com/stretchr/testify/mock"

type MockNodeManager struct {
	mock.Mock
}

func (m *MockNodeManager) BootstrapCoordinatorNode() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockNodeManager) GetCoordinatorNode() (*DataNodeInfo, bool) {
	args := m.Called()
	return args.Get(0).(*DataNodeInfo), args.Get(1).(bool)
}

func (m *MockNodeManager) AddCoordinatorNode(node *DataNodeInfo) {
	m.Called(node)
}

func (m *MockNodeManager) RemoveCoordinatorNode(nodeID string) {
	m.Called(nodeID)
}

func (m *MockNodeManager) ListCoordinatorNodes() ([]*DataNodeInfo, int64) {
	args := m.Called()
	return args.Get(0).([]*DataNodeInfo), args.Get(1).(int64)
}

func (m *MockNodeManager) GetCurrentVersion() int64 {
	return m.Called().Get(0).(int64)
}

func (m *MockNodeManager) GetNode(nodeID string) (*DataNodeInfo, bool) {
	args := m.Called(nodeID)
	return args.Get(0).(*DataNodeInfo), args.Get(1).(bool)
}

func (m *MockNodeManager) AddNode(node *DataNodeInfo) {
	m.Called(node)
}

func (m *MockNodeManager) RemoveNode(nodeID string) error {
	args := m.Called(nodeID)
	return args.Error(0)
}

func (m *MockNodeManager) UpdateNode(node *DataNodeInfo) error {
	args := m.Called(node)
	return args.Error(0)
}

func (m *MockNodeManager) ListNodes() ([]*DataNodeInfo, int64) {
	args := m.Called()
	return args.Get(0).([]*DataNodeInfo), args.Get(1).(int64)
}

func (m *MockNodeManager) GetUpdatesSince(sinceVersion int64) ([]NodeUpdate, int64, error) {
	args := m.Called(sinceVersion)
	return args.Get(0).([]NodeUpdate), args.Get(1).(int64), args.Error(2)
}

func (m *MockNodeManager) IsVersionTooOld(version int64) bool {
	args := m.Called(version)
	return args.Get(0).(bool)
}

func (m *MockNodeManager) SelectBestNodes(n int, self ...string) ([]*DataNodeInfo, error) {
	args := m.Called(n, self)
	return args.Get(0).([]*DataNodeInfo), args.Error(1)
}

func (m *MockNodeManager) GetAvailableNodesForChunk(replicaIDs []*DataNodeInfo) ([]*DataNodeInfo, bool) {
	args := m.Called(replicaIDs)
	return args.Get(0).([]*DataNodeInfo), args.Get(1).(bool)
}

func (m *MockNodeManager) ApplyHistory(updates []NodeUpdate) {
	m.Called(updates)
}

func (m *MockNodeManager) InitializeNodes(nodes []*DataNodeInfo, currentVersion int64) {
	m.Called(nodes, currentVersion)
}
