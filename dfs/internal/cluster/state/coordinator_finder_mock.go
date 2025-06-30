package state

import (
	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
)

type MockCoordinatorFinder struct {
	mock.Mock
}

func NewMockCoordinatorFinder() *MockCoordinatorFinder {
	return &MockCoordinatorFinder{
		Mock: mock.Mock{},
	}
}

func (m *MockCoordinatorFinder) GetCoordinator(nodeID string) (clients.ICoordinatorClient, bool) {
	return m.Called(nodeID).Get(0).(clients.ICoordinatorClient), m.Called(nodeID).Bool(1)
}

func (m *MockCoordinatorFinder) AddCoordinator(node *common.DataNodeInfo) {
	m.Called(node)
}

func (m *MockCoordinatorFinder) RemoveCoordinator(nodeID string) {
	m.Called(nodeID)
}

func (m *MockCoordinatorFinder) ListCoordinators() []*common.DataNodeInfo {
	return m.Called().Get(0).([]*common.DataNodeInfo)
}

func (m *MockCoordinatorFinder) GetLeaderCoordinator() (clients.ICoordinatorClient, bool) {
	return m.Called().Get(0).(clients.ICoordinatorClient), m.Called().Bool(1)
}

func (m *MockCoordinatorFinder) BootstrapCoordinator() error {
	return m.Called().Error(0)
}
