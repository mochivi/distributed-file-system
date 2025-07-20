package datanode_controllers

import "github.com/stretchr/testify/mock"

type MockOrphanedChunksGCController struct {
	mock.Mock
}

func (m *MockOrphanedChunksGCController) Run() error {
	args := m.Called()
	return args.Error(0)
}
