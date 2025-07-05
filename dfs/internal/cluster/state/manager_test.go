package state

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/assert"
)

func NewTestClusterStateManager(t *testing.T) {
	manager := NewClusterStateManager()
	assert.NotNil(t, manager, "NewClusterStateManager should not return nil")
	assert.NotNil(t, manager.store, "store should be initialized")
	assert.Zero(t, manager.version, "initial version should be 0")
}

func TestClusterStateManager_AddNode(t *testing.T) {
	node1 := &common.NodeInfo{ID: "node1", Host: "localhost", Port: 8080}
	node2 := &common.NodeInfo{ID: "node2", Host: "localhost", Port: 8081}

	tests := []struct {
		name            string
		nodeToAdd       *common.NodeInfo
		initialNodes    []*common.NodeInfo
		initialVersion  int64
		expectedCount   int
		expectedVersion int64
	}{
		{
			name:            "add first node",
			nodeToAdd:       node1,
			initialNodes:    []*common.NodeInfo{},
			initialVersion:  0,
			expectedCount:   1,
			expectedVersion: 1,
		},
		{
			name:            "add second node",
			nodeToAdd:       node2,
			initialNodes:    []*common.NodeInfo{node1},
			initialVersion:  1,
			expectedCount:   2,
			expectedVersion: 2,
		},
		{
			name:            "add existing node (should overwrite)",
			nodeToAdd:       &common.NodeInfo{ID: "node1", Host: "new-host"},
			initialNodes:    []*common.NodeInfo{node1},
			initialVersion:  1,
			expectedCount:   1,
			expectedVersion: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewClusterStateManager()
			manager.InitializeNodes(tt.initialNodes, tt.initialVersion)

			manager.AddNode(tt.nodeToAdd)

			nodes, version := manager.ListNodes()
			assert.Equal(t, tt.expectedCount, len(nodes))
			assert.Equal(t, tt.expectedVersion, version)

			gotNode, ok := manager.GetNode(tt.nodeToAdd.ID)
			assert.True(t, ok)
			assert.Equal(t, tt.nodeToAdd.Host, gotNode.Host)
		})
	}
}

func TestClusterStateManager_GetNode(t *testing.T) {
	node1 := &common.NodeInfo{ID: "node1"}
	manager := NewClusterStateManager()
	manager.AddNode(node1)

	tests := []struct {
		name         string
		nodeID       string
		expectFound  bool
		expectedNode *common.NodeInfo
	}{
		{"get existing node", "node1", true, node1},
		{"get non-existent node", "node2", false, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, found := manager.GetNode(tt.nodeID)
			assert.Equal(t, tt.expectFound, found)
			assert.Equal(t, tt.expectedNode, node)
		})
	}
}

func TestClusterStateManager_ListNodes(t *testing.T) {
	node1 := &common.NodeInfo{ID: "node1"}
	node2 := &common.NodeInfo{ID: "node2"}

	tests := []struct {
		name            string
		initialNodes    []*common.NodeInfo
		limit           []int
		expectedCount   int
		expectedVersion int64
	}{
		{"list with no nodes", []*common.NodeInfo{}, nil, 0, 0},
		{"list with one node", []*common.NodeInfo{node1}, nil, 1, 1},
		{"list with multiple nodes", []*common.NodeInfo{node1, node2}, nil, 2, 2},
		{"list with limit", []*common.NodeInfo{node1, node2}, []int{1}, 1, 2},
		{"list with limit greater than count", []*common.NodeInfo{node1, node2}, []int{5}, 2, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewClusterStateManager()
			for _, node := range tt.initialNodes {
				manager.AddNode(node)
			}

			nodes, version := manager.ListNodes(tt.limit...)
			assert.Equal(t, tt.expectedCount, len(nodes))
			assert.Equal(t, tt.expectedVersion, version)
		})
	}
}

func TestClusterStateManager_RemoveNode(t *testing.T) {
	node1 := &common.NodeInfo{ID: "node1"}
	node2 := &common.NodeInfo{ID: "node2"}

	tests := []struct {
		name            string
		nodeIDToRemove  string
		initialNodes    []*common.NodeInfo
		initialVersion  int64
		expectErr       bool
		expectedCount   int
		expectedVersion int64
	}{
		{"remove existing node", "node1", []*common.NodeInfo{node1, node2}, 2, false, 1, 3},
		{"remove non-existent node", "node3", []*common.NodeInfo{node1, node2}, 2, true, 2, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewClusterStateManager()
			manager.InitializeNodes(tt.initialNodes, tt.initialVersion)

			err := manager.RemoveNode(tt.nodeIDToRemove)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			nodes, version := manager.ListNodes()
			assert.Equal(t, tt.expectedCount, len(nodes))
			assert.Equal(t, tt.expectedVersion, version)
		})
	}
}

func TestClusterStateManager_UpdateNode(t *testing.T) {
	node1 := &common.NodeInfo{ID: "node1", Host: "host1"}
	updatedNode1 := &common.NodeInfo{ID: "node1", Host: "updated-host"}

	tests := []struct {
		name            string
		nodeToUpdate    *common.NodeInfo
		initialNodes    []*common.NodeInfo
		initialVersion  int64
		expectErr       bool
		expectedVersion int64
		expectedHost    string
	}{
		{"update existing node", updatedNode1, []*common.NodeInfo{node1}, 1, false, 2, "updated-host"},
		{"update non-existent node", &common.NodeInfo{ID: "node2"}, []*common.NodeInfo{node1}, 1, true, 1, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewClusterStateManager()
			manager.InitializeNodes(tt.initialNodes, tt.initialVersion)

			err := manager.UpdateNode(tt.nodeToUpdate)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				gotNode, ok := manager.GetNode(tt.nodeToUpdate.ID)
				assert.True(t, ok)
				assert.Equal(t, tt.expectedHost, gotNode.Host)
			}

			assert.Equal(t, tt.expectedVersion, manager.GetCurrentVersion())
		})
	}
}

func TestClusterStateManager_InitializeNodes(t *testing.T) {
	node1 := &common.NodeInfo{ID: "node1"}
	node2 := &common.NodeInfo{ID: "node2"}

	tests := []struct {
		name            string
		nodesToInit     []*common.NodeInfo
		versionToSet    int64
		expectedCount   int
		expectedVersion int64
	}{
		{"initialize with empty list", []*common.NodeInfo{}, 5, 0, 5},
		{"initialize with nodes", []*common.NodeInfo{node1, node2}, 10, 2, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewClusterStateManager()
			manager.InitializeNodes(tt.nodesToInit, tt.versionToSet)

			nodes, version := manager.ListNodes()
			assert.Equal(t, tt.expectedCount, len(nodes))
			assert.Equal(t, tt.expectedVersion, version)
		})
	}
}

func TestClusterStateManager_ApplyUpdates(t *testing.T) {
	node1 := &common.NodeInfo{ID: "node1"}
	node2 := &common.NodeInfo{ID: "node2"}
	node1Updated := &common.NodeInfo{ID: "node1", Host: "updated"}

	tests := []struct {
		name            string
		initialNodes    []*common.NodeInfo
		initialVersion  int64
		updates         []common.NodeUpdate
		expectedCount   int
		expectedVersion int64
		finalCheck      func(t *testing.T, m *clusterStateManager)
	}{
		{
			name:           "apply add update",
			initialNodes:   []*common.NodeInfo{},
			initialVersion: 0,
			updates: []common.NodeUpdate{
				{Version: 1, Type: common.NODE_ADDED, Node: node1, Timestamp: time.Now()},
			},
			expectedCount:   1,
			expectedVersion: 1,
			finalCheck: func(t *testing.T, m *clusterStateManager) {
				_, ok := m.GetNode("node1")
				assert.True(t, ok)
			},
		},
		{
			name:           "apply remove update",
			initialNodes:   []*common.NodeInfo{node1},
			initialVersion: 1,
			updates: []common.NodeUpdate{
				{Version: 2, Type: common.NODE_REMOVED, Node: node1, Timestamp: time.Now()},
			},
			expectedCount:   0,
			expectedVersion: 2,
			finalCheck: func(t *testing.T, m *clusterStateManager) {
				_, ok := m.GetNode("node1")
				assert.False(t, ok)
			},
		},
		{
			name:           "apply update update",
			initialNodes:   []*common.NodeInfo{node1},
			initialVersion: 1,
			updates: []common.NodeUpdate{
				{Version: 2, Type: common.NODE_UPDATED, Node: node1Updated, Timestamp: time.Now()},
			},
			expectedCount:   1,
			expectedVersion: 2,
			finalCheck: func(t *testing.T, m *clusterStateManager) {
				n, ok := m.GetNode("node1")
				assert.True(t, ok)
				assert.Equal(t, "updated", n.Host)
			},
		},
		{
			name:           "apply multiple updates",
			initialNodes:   []*common.NodeInfo{},
			initialVersion: 0,
			updates: []common.NodeUpdate{
				{Version: 1, Type: common.NODE_ADDED, Node: node1, Timestamp: time.Now()},
				{Version: 2, Type: common.NODE_ADDED, Node: node2, Timestamp: time.Now()},
				{Version: 3, Type: common.NODE_REMOVED, Node: node1, Timestamp: time.Now()},
			},
			expectedCount:   1,
			expectedVersion: 3,
			finalCheck: func(t *testing.T, m *clusterStateManager) {
				_, ok := m.GetNode("node1")
				assert.False(t, ok)
				_, ok = m.GetNode("node2")
				assert.True(t, ok)
			},
		},
		{
			name:           "updates with same version",
			initialNodes:   []*common.NodeInfo{},
			initialVersion: 0,
			updates: []common.NodeUpdate{
				{Version: 1, Type: common.NODE_ADDED, Node: node1, Timestamp: time.Now()},
				{Version: 1, Type: common.NODE_ADDED, Node: node2, Timestamp: time.Now()},
			},
			expectedCount:   2,
			expectedVersion: 1,
			finalCheck:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewClusterStateManager()
			manager.InitializeNodes(tt.initialNodes, tt.initialVersion)

			manager.ApplyUpdates(tt.updates)

			nodes, version := manager.ListNodes()
			assert.Equal(t, tt.expectedCount, len(nodes))
			assert.Equal(t, tt.expectedVersion, version)

			if tt.finalCheck != nil {
				tt.finalCheck(t, manager)
			}
		})
	}
}

func TestClusterStateManager_GetCurrentVersion(t *testing.T) {
	manager := NewClusterStateManager()
	assert.Equal(t, int64(0), manager.GetCurrentVersion())

	manager.AddNode(&common.NodeInfo{ID: "node1"})
	assert.Equal(t, int64(1), manager.GetCurrentVersion())

	manager.InitializeNodes([]*common.NodeInfo{}, 10)
	assert.Equal(t, int64(10), manager.GetCurrentVersion())
}

func TestClusterStateManager_Concurrency(t *testing.T) {
	manager := NewClusterStateManager()
	var wg sync.WaitGroup
	numGoroutines := 100

	// Test concurrent additions
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			node := &common.NodeInfo{ID: fmt.Sprintf("node-%d", i)}
			manager.AddNode(node)
		}(i)
	}
	wg.Wait()

	nodes, version := manager.ListNodes()
	assert.Equal(t, numGoroutines, len(nodes))
	assert.Equal(t, int64(numGoroutines), version)

	// Test concurrent updates and removals
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				// Remove even nodes
				err := manager.RemoveNode(fmt.Sprintf("node-%d", i))
				assert.NoError(t, err)
			} else {
				// Update odd nodes
				node := &common.NodeInfo{ID: fmt.Sprintf("node-%d", i), Host: "updated"}
				err := manager.UpdateNode(node)
				assert.NoError(t, err)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			manager.ListNodes()
			manager.GetNode("node-1") // a node that will be updated
		}()
	}

	wg.Wait()

	nodes, version = manager.ListNodes()
	assert.Equal(t, numGoroutines/2, len(nodes))
	assert.Equal(t, int64(numGoroutines*2), version)

	for _, node := range nodes {
		// All remaining nodes should be the updated odd nodes
		assert.Equal(t, "updated", node.Host)
		_, err := fmt.Sscanf(node.ID, "node-%d", new(int))
		assert.NoError(t, err, "node ID should have integer suffix")
	}
}
