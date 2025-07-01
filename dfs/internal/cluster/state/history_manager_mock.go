package state

import (
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
)

type MockClusterStateHistoryManager struct {
	mock.Mock
}

func (m *MockClusterStateHistoryManager) ListNodes(n ...int) ([]*common.DataNodeInfo, int64) {
	args := m.Called(n)
	if args.Get(0) == nil {
		return nil, args.Get(1).(int64)
	}
	return args.Get(0).([]*common.DataNodeInfo), args.Get(1).(int64)
}

func (m *MockClusterStateHistoryManager) GetNode(nodeID string) (*common.DataNodeInfo, bool) {
	args := m.Called(nodeID)
	if args.Get(0) == nil {
		return nil, args.Bool(1)
	}
	return args.Get(0).(*common.DataNodeInfo), args.Bool(1)
}

func (m *MockClusterStateHistoryManager) AddNode(node *common.DataNodeInfo) {
	m.Called(node)
}

func (m *MockClusterStateHistoryManager) RemoveNode(nodeID string) error {
	args := m.Called(nodeID)
	return args.Error(0)
}

func (m *MockClusterStateHistoryManager) UpdateNode(node *common.DataNodeInfo) error {
	args := m.Called(node)
	return args.Error(0)
}

func (m *MockClusterStateHistoryManager) InitializeNodes(nodes []*common.DataNodeInfo, version int64) {
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

func (m *MockClusterStateHistoryManager) GetAvailableNodesForChunk(replicaIDs []*common.DataNodeInfo) ([]*common.DataNodeInfo, bool) {
	args := m.Called(replicaIDs)
	if args.Get(0) == nil {
		return nil, args.Bool(1)
	}
	return args.Get(0).([]*common.DataNodeInfo), args.Bool(1)
}
