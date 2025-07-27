package client_pool

import (
	"sync"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/stretchr/testify/mock"
)

// MockClientPool is a mock implementation of the ClientPool for testing.
type MockClientPool struct {
	mock.Mock
	clients []clients.IDataNodeClient
	mu      sync.Mutex
}

func NewMockClientPool(clients ...clients.IDataNodeClient) *MockClientPool {
	return &MockClientPool{
		clients: clients,
	}
}

func (m *MockClientPool) GetClient() clients.IDataNodeClient {
	args := m.Called()

	var client clients.IDataNodeClient
	switch c := args.Get(0).(type) {
	case clients.IDataNodeClient:
		client = c
	case nil:
		client = nil
	default:
		client = nil
	}

	return client
}

func (m *MockClientPool) GetClientWithRetry(clientConnectionFunc clientConnectionFunc) (clients.IDataNodeClient, string, error) {
	args := m.Called(clientConnectionFunc)

	var client clients.IDataNodeClient
	switch c := args.Get(0).(type) {
	case clients.IDataNodeClient:
		client = c
	case nil:
		client = nil
	default:
		client = nil
	}

	return client, args.String(1), args.Error(2)
}

func (m *MockClientPool) GetRemoveClientWithRetry(clientConnectionFunc clientConnectionFunc) (clients.IDataNodeClient, string, error) {
	args := m.Called(clientConnectionFunc)

	var client clients.IDataNodeClient
	switch c := args.Get(0).(type) {
	case clients.IDataNodeClient:
		client = c
	case nil:
		client = nil
	default:
		client = nil
	}

	return client, args.String(1), args.Error(2)
}

func (m *MockClientPool) Len() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockClientPool) Close() {
	m.Called()
}
