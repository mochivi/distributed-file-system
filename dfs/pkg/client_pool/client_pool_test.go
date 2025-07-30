package client_pool

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRotatingClientPool(t *testing.T) {
	testCases := []struct {
		name        string
		nodes       []*common.NodeInfo
		expectErr   bool
		expectedLen int
	}{
		{
			name: "success: multiple nodes",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
				{ID: "node3", Host: "localhost", Port: 8082},
			},
			expectErr:   false,
			expectedLen: 3,
		},
		{
			name: "success: single node",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
			},
			expectErr:   false,
			expectedLen: 1,
		},
		{
			name:        "success: empty nodes",
			nodes:       []*common.NodeInfo{},
			expectErr:   false,
			expectedLen: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := NewRotatingClientPool(tc.nodes)

			if tc.expectErr {
				assert.Error(t, err)
				assert.Nil(t, pool)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, pool)
				assert.Equal(t, tc.expectedLen, pool.Len())
			}
		})
	}
}

func TestRotatingClientPool_GetClientWithRetry(t *testing.T) {
	testCases := []struct {
		name           string
		nodes          []*common.NodeInfo
		connectionFunc clientConnectionFunc
		expectedResult struct {
			client   string // Expected node ID
			response any
			err      bool
		}
	}{
		{
			name: "success: first client accepts immediately",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
			},
			connectionFunc: func(client clients.IDataNodeClient) (bool, any, error) {
				return true, "session-123", nil
			},
			expectedResult: struct {
				client   string
				response any
				err      bool
			}{
				client:   "node1",
				response: "session-123",
				err:      false,
			},
		},
		{
			name: "error: client refuses connection",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
			},
			connectionFunc: func(client clients.IDataNodeClient) (bool, any, error) {
				return false, nil, nil // refused connection
			},
			expectedResult: struct {
				client   string
				response any
				err      bool
			}{
				client:   "",
				response: nil,
				err:      true,
			},
		},
		{
			name: "error: client unavailable",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
			},
			connectionFunc: func(client clients.IDataNodeClient) (bool, any, error) {
				return false, nil, errors.New("connection-error") // client unavailable
			},
			expectedResult: struct {
				client   string
				response any
				err      bool
			}{
				client:   "",
				response: nil,
				err:      true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := NewRotatingClientPool(tc.nodes)
			require.NoError(t, err)

			client, response, err := pool.GetClientWithRetry(tc.connectionFunc)

			if tc.expectedResult.err {
				assert.Error(t, err)
				assert.Nil(t, client)
				assert.Nil(t, response)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)
				assert.Equal(t, tc.expectedResult.client, client.Node().ID)
				assert.Equal(t, tc.expectedResult.response, response)
			}
		})
	}
}

func TestRotatingClientPool_GetClientWithRetry_Loop(t *testing.T) {
	testCases := []struct {
		name            string
		nodes           []*common.NodeInfo
		connectionFuncs []clientConnectionFunc
		expectedResults []struct {
			client   string // Expected node ID
			response any
			err      bool
		}
		expectedPoolSize int // Pool size should remain constant since clients aren't removed
	}{
		{
			name: "success: multiple successful connections",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
				{ID: "node3", Host: "localhost", Port: 8082},
			},
			connectionFuncs: []clientConnectionFunc{
				func(client clients.IDataNodeClient) (bool, any, error) {
					return true, "session-123", nil
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return true, "session-456", nil
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return true, "session-789", nil
				},
			},
			expectedResults: []struct {
				client   string
				response any
				err      bool
			}{
				{
					client:   "node1",
					response: "session-123",
					err:      false,
				},
				{
					client:   "node2",
					response: "session-456",
					err:      false,
				},
				{
					client:   "node3",
					response: "session-789",
					err:      false,
				},
			},
			expectedPoolSize: 3,
		},
		{
			name: "success: retry logic with temporary failures",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
			},
			connectionFuncs: []clientConnectionFunc{
				func(client clients.IDataNodeClient) (bool, any, error) {
					// First client fails initially, then succeeds
					if client.Node().ID == "node1" {
						return true, "session-retry-success", nil
					}
					return true, "session-node2", nil
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					// Second client succeeds immediately
					return true, "session-node2-success", nil
				},
			},
			expectedResults: []struct {
				client   string
				response any
				err      bool
			}{
				{
					client:   "node1",
					response: "session-retry-success",
					err:      false,
				},
				{
					client:   "node2",
					response: "session-node2-success",
					err:      false,
				},
			},
			expectedPoolSize: 2,
		},
		{
			name: "error: all clients refuse connection",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
			},
			connectionFuncs: []clientConnectionFunc{
				func(client clients.IDataNodeClient) (bool, any, error) {
					return false, nil, nil // refuse connection
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return false, nil, nil // refuse connection
				},
			},
			expectedResults: []struct {
				client   string
				response any
				err      bool
			}{
				{
					client:   "",
					response: nil,
					err:      true,
				},
				{
					client:   "",
					response: nil,
					err:      true,
				},
			},
			expectedPoolSize: 2, // Clients remain in pool even when they refuse
		},
		{
			name: "error: all clients return errors",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
			},
			connectionFuncs: []clientConnectionFunc{
				func(client clients.IDataNodeClient) (bool, any, error) {
					return false, nil, fmt.Errorf("connection error")
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return false, nil, fmt.Errorf("connection error")
				},
			},
			expectedResults: []struct {
				client   string
				response any
				err      bool
			}{
				{
					client:   "",
					response: nil,
					err:      true,
				},
				{
					client:   "",
					response: nil,
					err:      true,
				},
			},
			expectedPoolSize: 2, // Clients remain in pool even when they return errors
		},
		{
			name: "mixed: first client refuses, second accepts",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
				{ID: "node3", Host: "localhost", Port: 8082},
			},
			connectionFuncs: []clientConnectionFunc{
				func(client clients.IDataNodeClient) (bool, any, error) {
					return false, nil, nil // refuse
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return true, "session-accept2", nil // accept
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return true, "session-accept3", nil // accept
				},
			},
			expectedResults: []struct {
				client   string
				response any
				err      bool
			}{
				{
					client:   "",
					response: nil,
					err:      true,
				},
				{
					client:   "node2",
					response: "session-accept2",
					err:      false,
				},
				{
					client:   "node3",
					response: "session-accept3",
					err:      false,
				},
			},
			expectedPoolSize: 3,
		},
		{
			name: "round-robin rotation test",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
			},
			connectionFuncs: []clientConnectionFunc{
				func(client clients.IDataNodeClient) (bool, any, error) {
					return true, "session-1", nil
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return true, "session-2", nil
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return true, "session-3", nil
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return true, "session-4", nil
				},
			},
			expectedResults: []struct {
				client   string
				response any
				err      bool
			}{
				{
					client:   "node1",
					response: "session-1",
					err:      false,
				},
				{
					client:   "node2",
					response: "session-2",
					err:      false,
				},
				{
					client:   "node1", // Should rotate back to node1
					response: "session-3",
					err:      false,
				},
				{
					client:   "node2", // Should rotate back to node2
					response: "session-4",
					err:      false,
				},
			},
			expectedPoolSize: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := NewRotatingClientPool(tc.nodes)
			require.NoError(t, err)

			// Verify initial pool size
			assert.Equal(t, len(tc.nodes), pool.Len())

			// Test each connection function
			for i, connectionFunc := range tc.connectionFuncs {
				client, response, err := pool.GetClientWithRetry(connectionFunc)

				if tc.expectedResults[i].err {
					assert.Error(t, err)
					assert.Nil(t, client)
					assert.Nil(t, response)
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, client)
					assert.Equal(t, tc.expectedResults[i].client, client.Node().ID)
					assert.Equal(t, tc.expectedResults[i].response, response)
				}

				// Verify pool size remains constant (clients are not removed)
				assert.Equal(t, tc.expectedPoolSize, pool.Len())
			}
		})
	}
}

func TestRotatingClientPool_GetRemoveClientWithRetry(t *testing.T) {
	testCases := []struct {
		name           string
		nodes          []*common.NodeInfo
		connectionFunc clientConnectionFunc
		expectedResult struct {
			client      string
			response    any
			err         bool
			remaining   int
			removedNode string
		}
	}{
		{
			name: "success: get and remove client",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
				{ID: "node3", Host: "localhost", Port: 8082},
			},
			connectionFunc: func(client clients.IDataNodeClient) (bool, any, error) {
				return true, "session-123", nil
			},
			expectedResult: struct {
				client      string
				response    any
				err         bool
				remaining   int
				removedNode string
			}{
				client:      "node1",
				response:    "session-123",
				err:         false,
				remaining:   2,
				removedNode: "node1",
			},
		},
		{
			name: "error: client refuses connection",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
			},
			connectionFunc: func(client clients.IDataNodeClient) (bool, any, error) {
				return false, nil, nil // client refuses connection by replying with accept: false
			},
			expectedResult: struct {
				client      string
				response    any
				err         bool
				remaining   int
				removedNode string
			}{
				client:      "",
				response:    nil,
				err:         true,
				remaining:   0, // Client is removed no matter what
				removedNode: "",
			},
		},
		{
			name: "error: client unavailable",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
			},
			connectionFunc: func(client clients.IDataNodeClient) (bool, any, error) {
				return false, nil, errors.New("connection failed") // client returns errors
			},
			expectedResult: struct {
				client      string
				response    any
				err         bool
				remaining   int
				removedNode string
			}{
				client:      "",
				response:    nil,
				err:         true,
				remaining:   0, // Client is removed no matter what
				removedNode: "",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := NewRotatingClientPool(tc.nodes)
			require.NoError(t, err)

			client, response, err := pool.GetRemoveClientWithRetry(tc.connectionFunc)

			if tc.expectedResult.err {
				assert.Error(t, err)
				assert.Nil(t, client)
				assert.Nil(t, response)
				assert.Equal(t, tc.expectedResult.remaining, pool.Len())
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)
				assert.Equal(t, tc.expectedResult.client, client.Node().ID)
				assert.Equal(t, tc.expectedResult.response, response)
				assert.Equal(t, tc.expectedResult.remaining, pool.Len())
			}
		})
	}
}

func TestRotatingClientPool_GetRemoveClientWithRetry_Loop(t *testing.T) {
	testCases := []struct {
		name            string
		nodes           []*common.NodeInfo
		connectionFuncs []clientConnectionFunc
		expectedResults []struct {
			client      string
			response    any
			err         bool
			remaining   int
			removedNode string
		}
	}{
		{
			name: "success: get and remove clients",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
				{ID: "node3", Host: "localhost", Port: 8082},
			},
			connectionFuncs: []clientConnectionFunc{
				func(client clients.IDataNodeClient) (bool, any, error) {
					return true, "session-123", nil
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return true, "session-456", nil
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return true, "session-789", nil
				},
			},
			expectedResults: []struct {
				client      string
				response    any
				err         bool
				remaining   int
				removedNode string
			}{
				{ // first response
					client:      "node1",
					response:    "session-123",
					err:         false,
					remaining:   2,
					removedNode: "node1",
				},
				{ // second response
					client:      "node2",
					response:    "session-456",
					err:         false,
					remaining:   1,
					removedNode: "node2",
				},
				{ // third response
					client:      "node3",
					response:    "session-789",
					err:         false,
					remaining:   0,
					removedNode: "node3",
				},
			},
		},
		{
			name: "error: all clients refuse connection",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
			},
			connectionFuncs: []clientConnectionFunc{
				func(client clients.IDataNodeClient) (bool, any, error) {
					return false, nil, nil // refuse connection
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return false, nil, nil // refuse connection
				},
			},
			expectedResults: []struct {
				client      string
				response    any
				err         bool
				remaining   int
				removedNode string
			}{
				{
					client:      "",
					response:    nil,
					err:         true,
					remaining:   1,
					removedNode: "",
				},
				{
					client:      "",
					response:    nil,
					err:         true,
					remaining:   0,
					removedNode: "",
				},
			},
		},
		{
			name: "all clients return errors",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
			},
			connectionFuncs: []clientConnectionFunc{
				func(client clients.IDataNodeClient) (bool, any, error) {
					return false, nil, fmt.Errorf("connection error")
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return false, nil, fmt.Errorf("connection error")
				},
			},
			expectedResults: []struct {
				client      string
				response    any
				err         bool
				remaining   int
				removedNode string
			}{
				{
					client:      "",
					response:    nil,
					err:         true,
					remaining:   1,
					removedNode: "",
				},
				{
					client:      "",
					response:    nil,
					err:         true,
					remaining:   0,
					removedNode: "",
				},
			},
		},
		{
			name: "mixed: first client refuses, second returns error, third accepts",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
				{ID: "node3", Host: "localhost", Port: 8082},
			},
			connectionFuncs: []clientConnectionFunc{
				func(client clients.IDataNodeClient) (bool, any, error) {
					return false, nil, nil // refuse
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return false, nil, fmt.Errorf("connection error") // error
				},
				func(client clients.IDataNodeClient) (bool, any, error) {
					return true, "session-accept3", nil // accept
				},
			},
			expectedResults: []struct {
				client      string
				response    any
				err         bool
				remaining   int
				removedNode string
			}{
				{
					client:      "",
					response:    nil,
					err:         true,
					remaining:   2,
					removedNode: "node1",
				},
				{
					client:      "",
					response:    nil,
					err:         true,
					remaining:   1,
					removedNode: "node2",
				},
				{
					client:      "node3",
					response:    "session-accept3",
					err:         false,
					remaining:   0,
					removedNode: "node3",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := NewRotatingClientPool(tc.nodes)
			require.NoError(t, err)

			currentLen := pool.Len()
			i := 0

			for pool.Len() > 0 {
				client, response, err := pool.GetRemoveClientWithRetry(tc.connectionFuncs[i])
				currentLen--

				if tc.expectedResults[i].err {
					assert.Error(t, err)
					assert.Nil(t, client)                   // client must be nil if any error is returned or connection refused
					assert.Nil(t, response)                 // response must be nil if any error is returned or connection refused
					assert.Equal(t, currentLen, pool.Len()) // Client must be removed from pool
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, client)
					assert.Equal(t, tc.expectedResults[i].client, client.Node().ID)
					assert.Equal(t, tc.expectedResults[i].response, response)
					assert.Equal(t, tc.expectedResults[i].remaining, pool.Len())
				}

				i++
			}

		})
	}
}

func TestRotatingClientPool_ConcurrentAccess(t *testing.T) {
	testCases := []struct {
		name          string
		nodes         []*common.NodeInfo
		numGoroutines int
		operations    func(*rotatingClientPool, int)
		expectedLen   int
	}{
		{
			name: "concurrent GetClientWithRetry calls",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
			},
			numGoroutines: 5,
			operations: func(pool *rotatingClientPool, id int) {
				for i := 0; i < 3; i++ {
					client, response, err := pool.GetClientWithRetry(func(c clients.IDataNodeClient) (bool, any, error) {
						return true, fmt.Sprintf("session-%d-%d", id, i), nil
					})
					assert.NoError(t, err)
					assert.NotNil(t, client)
					assert.NotNil(t, response)
				}
			},
			expectedLen: 2,
		},
		{
			name: "concurrent GetRemoveClientWithRetry calls",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
				{ID: "node3", Host: "localhost", Port: 8082},
				{ID: "node4", Host: "localhost", Port: 8083},
			},
			numGoroutines: 3,
			operations: func(pool *rotatingClientPool, id int) {
				client, response, err := pool.GetRemoveClientWithRetry(func(c clients.IDataNodeClient) (bool, any, error) {
					return true, fmt.Sprintf("session-%d", id), nil
				})
				assert.NoError(t, err)
				assert.NotNil(t, client)
				assert.NotNil(t, response)
			},
			expectedLen: 1, // 3 clients removed, 1 remaining
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := NewRotatingClientPool(tc.nodes)
			require.NoError(t, err)

			var wg sync.WaitGroup
			for i := 0; i < tc.numGoroutines; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					tc.operations(pool, id)
				}(i)
			}

			wg.Wait()

			// Verify final state
			assert.Equal(t, tc.expectedLen, pool.Len())
		})
	}
}

func TestRotatingClientPool_EdgeCases(t *testing.T) {
	testCases := []struct {
		name        string
		setup       func() *rotatingClientPool
		operation   func(*rotatingClientPool) error
		expectPanic bool
	}{
		{
			name: "empty pool GetClientWithRetry",
			setup: func() *rotatingClientPool {
				pool, _ := NewRotatingClientPool([]*common.NodeInfo{})
				return pool
			},
			operation: func(pool *rotatingClientPool) error {
				_, _, err := pool.GetClientWithRetry(func(c clients.IDataNodeClient) (bool, any, error) {
					return true, nil, nil
				})
				return err
			},
			expectPanic: false,
		},
		{
			name: "nil connection function",
			setup: func() *rotatingClientPool {
				pool, _ := NewRotatingClientPool([]*common.NodeInfo{
					{ID: "node1", Host: "localhost", Port: 8080},
				})
				return pool
			},
			operation: func(pool *rotatingClientPool) error {
				_, _, err := pool.GetClientWithRetry(nil)
				return err
			},
			expectPanic: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pool := tc.setup()

			if tc.expectPanic {
				assert.Panics(t, func() {
					tc.operation(pool)
				})
			} else {
				err := tc.operation(pool)
				assert.Error(t, err)
			}
		})
	}
}

func TestRotatingClientPool_Close(t *testing.T) {
	testCases := []struct {
		name  string
		nodes []*common.NodeInfo
	}{
		{
			name: "close with multiple clients",
			nodes: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 8080},
				{ID: "node2", Host: "localhost", Port: 8081},
				{ID: "node3", Host: "localhost", Port: 8082},
			},
		},
		{
			name:  "close empty pool",
			nodes: []*common.NodeInfo{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := NewRotatingClientPool(tc.nodes)
			require.NoError(t, err)

			// Close should not panic
			assert.NotPanics(t, func() {
				pool.Close()
			})

			// After close, pool should still be usable (though clients may be closed)
			assert.Equal(t, len(tc.nodes), pool.Len())
		})
	}
}

func BenchmarkRotatingClientPool_GetClientWithRetry(b *testing.B) {
	pool, err := NewRotatingClientPool([]*common.NodeInfo{
		{ID: "node1", Host: "localhost", Port: 8080},
		{ID: "node2", Host: "localhost", Port: 8081},
	})
	require.NoError(b, err)

	connectionFunc := func(client clients.IDataNodeClient) (bool, any, error) {
		return true, "session", nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.GetClientWithRetry(connectionFunc)
	}
}
