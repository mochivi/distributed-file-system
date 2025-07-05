package state

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestNewCoordinatorFinder(t *testing.T) {
	finder := NewCoordinatorFinder()
	assert.NotNil(t, finder)
	assert.NotNil(t, finder.nodes)
	assert.Empty(t, finder.nodes)
	assert.NotNil(t, finder.clientConnectionFunc)
}

func TestCoordinatorFinder_AddRemoveList(t *testing.T) {
	finder := NewCoordinatorFinder()
	node1 := &common.NodeInfo{ID: "coord1", Host: "localhost", Port: 8080}
	node2 := &common.NodeInfo{ID: "coord2", Host: "localhost", Port: 8081}

	// Initial state
	assert.Empty(t, finder.ListCoordinators())

	// Add one
	finder.AddCoordinator(node1)
	assert.Len(t, finder.ListCoordinators(), 1)
	assert.Contains(t, finder.nodes, "coord1")

	// Add another
	finder.AddCoordinator(node2)
	assert.Len(t, finder.ListCoordinators(), 2)
	assert.ElementsMatch(t, []*common.NodeInfo{node1, node2}, finder.ListCoordinators())

	// Remove one
	finder.RemoveCoordinator("coord1")
	assert.Len(t, finder.ListCoordinators(), 1)
	assert.NotContains(t, finder.nodes, "coord1")
	assert.Contains(t, finder.nodes, "coord2")

	// Remove non-existent
	finder.RemoveCoordinator("non-existent")
	assert.Len(t, finder.ListCoordinators(), 1)
}

func TestCoordinatorFinder_GetCoordinator(t *testing.T) {
	node1 := &common.NodeInfo{ID: "coord1", Host: "localhost", Port: 8080}

	testCases := []struct {
		name           string
		nodeIDToGet    string
		initialNodes   []*common.NodeInfo
		setupFinder    func(finder *coordinatorFinder)
		expectFound    bool
		expectedNodeID string
	}{
		{
			name:         "get existing coordinator",
			nodeIDToGet:  "coord1",
			initialNodes: []*common.NodeInfo{node1},
			setupFinder: func(finder *coordinatorFinder) {
				finder.SetClientConnectionFunc(func(node *common.NodeInfo, opts ...grpc.DialOption) (clients.ICoordinatorClient, error) {
					return clients.NewMockCoordinatorClient(node), nil
				})
			},
			expectFound:    true,
			expectedNodeID: "coord1",
		},
		{
			name:         "get non-existent coordinator",
			nodeIDToGet:  "non-existent",
			initialNodes: []*common.NodeInfo{node1},
			setupFinder:  func(finder *coordinatorFinder) {},
			expectFound:  false,
		},
		{
			name:         "connection func returns error",
			nodeIDToGet:  "coord1",
			initialNodes: []*common.NodeInfo{node1},
			setupFinder: func(finder *coordinatorFinder) {
				finder.SetClientConnectionFunc(func(node *common.NodeInfo, opts ...grpc.DialOption) (clients.ICoordinatorClient, error) {
					return nil, errors.New("connection failed")
				})
			},
			expectFound: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			finder := NewCoordinatorFinder()
			for _, node := range tc.initialNodes {
				finder.AddCoordinator(node)
			}
			tc.setupFinder(finder)

			client, ok := finder.GetCoordinator(tc.nodeIDToGet)

			assert.Equal(t, tc.expectFound, ok)
			if tc.expectFound {
				assert.NotNil(t, client)
				assert.Equal(t, tc.expectedNodeID, client.Node().ID)
			} else {
				assert.Nil(t, client)
			}
		})
	}
}

func TestCoordinatorFinder_GetLeaderCoordinator(t *testing.T) {
	nodeHealthy := &common.NodeInfo{ID: "healthy", Status: common.NodeHealthy}
	nodeUnhealthy := &common.NodeInfo{ID: "unhealthy", Status: common.NodeUnhealthy}
	nodeFailsConnect := &common.NodeInfo{ID: "fails-connect", Status: common.NodeHealthy}

	testCases := []struct {
		name             string
		initialNodes     []*common.NodeInfo
		setupFinder      func(finder *coordinatorFinder)
		expectFound      bool
		expectedLeaderID string
	}{
		{
			name:         "no coordinators",
			initialNodes: []*common.NodeInfo{},
			setupFinder:  func(finder *coordinatorFinder) {},
			expectFound:  false,
		},
		{
			name:         "only unhealthy coordinators",
			initialNodes: []*common.NodeInfo{nodeUnhealthy},
			setupFinder:  func(finder *coordinatorFinder) {},
			expectFound:  false,
		},
		{
			name: "skips failing node and finds healthy one",
			// Add in non-alphabetical order to test iteration
			initialNodes: []*common.NodeInfo{nodeFailsConnect, nodeUnhealthy, nodeHealthy},
			setupFinder: func(finder *coordinatorFinder) {
				finder.SetClientConnectionFunc(func(node *common.NodeInfo, opts ...grpc.DialOption) (clients.ICoordinatorClient, error) {
					if node.ID == "fails-connect" {
						return nil, errors.New("connection failed")
					}
					return clients.NewMockCoordinatorClient(node), nil
				})
			},
			expectFound:      true,
			expectedLeaderID: "healthy",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			finder := NewCoordinatorFinder()
			for _, node := range tc.initialNodes {
				finder.AddCoordinator(node)
			}
			tc.setupFinder(finder)

			client, ok := finder.GetLeaderCoordinator()

			assert.Equal(t, tc.expectFound, ok)
			if tc.expectFound {
				assert.NotNil(t, client)
				assert.Equal(t, tc.expectedLeaderID, client.Node().ID)
			} else {
				assert.Nil(t, client)
			}
		})
	}
}

func TestCoordinatorFinder_Concurrency(t *testing.T) {
	finder := NewCoordinatorFinder()
	var wg sync.WaitGroup
	numRoutines := 100

	// Mock connection func
	finder.SetClientConnectionFunc(func(node *common.NodeInfo, opts ...grpc.DialOption) (clients.ICoordinatorClient, error) {
		return clients.NewMockCoordinatorClient(node), nil
	})

	// Concurrent Add/Remove
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("node-%d", i)
			node := &common.NodeInfo{ID: nodeID, Status: common.NodeHealthy}
			finder.AddCoordinator(node)
			if i%2 == 0 {
				finder.RemoveCoordinator(nodeID)
			}
		}(i)
	}

	// Concurrent Reads
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			finder.ListCoordinators()
			finder.GetLeaderCoordinator()
			finder.GetCoordinator("node-1") // May or may not exist
		}()
	}

	wg.Wait()

	// Assert final state
	// 50 nodes should remain (the odd-numbered ones)
	assert.Len(t, finder.ListCoordinators(), 50)
	client, ok := finder.GetLeaderCoordinator()
	assert.True(t, ok)
	assert.NotNil(t, client)
}
