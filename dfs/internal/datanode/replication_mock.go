package datanode

import (
	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
)

type MockParalellReplicationService struct {
	mock.Mock
}

func (m *MockParalellReplicationService) Replicate(clients []*clients.DataNodeClient, chunkHeader common.ChunkHeader, data []byte, requiredReplicas int) ([]*common.NodeInfo, error) {
	args := m.Called(clients, chunkHeader, data, requiredReplicas)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*common.NodeInfo), args.Error(1)
}
