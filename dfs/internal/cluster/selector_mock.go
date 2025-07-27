package cluster

import (
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
)

type MockNodeSelector struct {
	mock.Mock
}

func (m *MockNodeSelector) SelectBestNodes(numChunks int, self *common.NodeInfo) ([]*common.NodeInfo, bool) {
	args := m.Called(numChunks, self)
	if args.Get(0) == nil {
		return nil, false
	}
	return args.Get(0).([]*common.NodeInfo), args.Bool(1)
}
