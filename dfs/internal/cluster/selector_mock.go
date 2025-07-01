package cluster

import (
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
)

type MockNodeSelector struct {
	mock.Mock
}

func (m *MockNodeSelector) SelectBestNodes(numChunks int) ([]*common.DataNodeInfo, bool) {
	args := m.Called(numChunks)
	return args.Get(0).([]*common.DataNodeInfo), args.Bool(1)
}
