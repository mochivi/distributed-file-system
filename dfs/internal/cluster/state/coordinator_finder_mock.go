package state

import (
	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
)

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
