package state

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/stretchr/testify/assert"
)

func newTestHistoryManager(maxHistory int) *clusterStateHistoryManager {
	return NewClusterStateHistoryManager(config.ClusterStateHistoryManagerConfig{
		MaxHistorySize: maxHistory,
	})
}

func TestNewClusterStateHistoryManager(t *testing.T) {
	t.Run("with valid config", func(t *testing.T) {
		manager := newTestHistoryManager(10)
		assert.NotNil(t, manager)
		assert.NotNil(t, manager.store)
		assert.Equal(t, 10, manager.config.MaxHistorySize)
		assert.Len(t, manager.history, 10)
		assert.Zero(t, manager.version)
	})

	t.Run("with zero history size", func(t *testing.T) {
		manager := NewClusterStateHistoryManager(config.ClusterStateHistoryManagerConfig{MaxHistorySize: 0})
		assert.Equal(t, 100, manager.config.MaxHistorySize, "should use default size")
		assert.Len(t, manager.history, 100)
	})
}

func TestClusterStateHistoryManager_AddNode(t *testing.T) {
	manager := newTestHistoryManager(5)
	node1 := &common.NodeInfo{ID: "node1"}
	manager.AddNode(node1)

	// Check node
	gotNode, ok := manager.GetNode("node1")
	assert.True(t, ok)
	assert.Equal(t, node1, gotNode)

	// Check version
	assert.Equal(t, int64(1), manager.GetCurrentVersion())

	// Check history
	updates, _, _ := manager.GetUpdatesSince(0)
	assert.Len(t, updates, 1)
	assert.Equal(t, int64(1), updates[0].Version)
	assert.Equal(t, common.NODE_ADDED, updates[0].Type)
	assert.Equal(t, "node1", updates[0].Node.ID)
}

func TestClusterStateHistoryManager_RemoveNode(t *testing.T) {
	manager := newTestHistoryManager(5)
	node1 := &common.NodeInfo{ID: "node1"}
	manager.AddNode(node1) // Version 1

	err := manager.RemoveNode("node1") // Version 2
	assert.NoError(t, err)

	_, ok := manager.GetNode("node1")
	assert.False(t, ok)
	assert.Equal(t, int64(2), manager.GetCurrentVersion())

	updates, _, _ := manager.GetUpdatesSince(1)
	assert.Len(t, updates, 1)
	assert.Equal(t, int64(2), updates[0].Version)
	assert.Equal(t, common.NODE_REMOVED, updates[0].Type)

	err = manager.RemoveNode("node_nonexistent")
	assert.Error(t, err)
	assert.Equal(t, int64(2), manager.GetCurrentVersion(), "version should not change on error")
}

func TestClusterStateHistoryManager_UpdateNode(t *testing.T) {
	manager := newTestHistoryManager(5)
	node1 := &common.NodeInfo{ID: "node1", Host: "host1"}
	updatedNode1 := &common.NodeInfo{ID: "node1", Host: "host2"}
	manager.AddNode(node1) // Version 1

	err := manager.UpdateNode(updatedNode1) // Version 2
	assert.NoError(t, err)

	gotNode, ok := manager.GetNode("node1")
	assert.True(t, ok)
	assert.Equal(t, "host2", gotNode.Host)
	assert.Equal(t, int64(2), manager.GetCurrentVersion())

	updates, _, _ := manager.GetUpdatesSince(1)
	assert.Len(t, updates, 1)
	assert.Equal(t, int64(2), updates[0].Version)
	assert.Equal(t, common.NODE_UPDATED, updates[0].Type)

	err = manager.UpdateNode(&common.NodeInfo{ID: "node_nonexistent"})
	assert.Error(t, err)
	assert.Equal(t, int64(2), manager.GetCurrentVersion())
}

func TestClusterStateHistoryManager_GetUpdatesSince(t *testing.T) {
	manager := newTestHistoryManager(5)
	for i := 1; i <= 7; i++ {
		manager.AddNode(&common.NodeInfo{ID: fmt.Sprintf("node%d", i)})
	}
	assert.Equal(t, int64(7), manager.GetCurrentVersion())
	// History should contain versions 3, 4, 5, 6, 7

	tests := []struct {
		name            string
		sinceVersion    int64
		expectErr       bool
		expectedUpdates int
	}{
		{"version too old", 1, true, 0},
		{"oldest version in history", 2, true, 0},
		{"valid version", 3, false, 4},         // updates for 4, 5, 6, 7
		{"another valid version", 5, false, 2}, // updates for 6, 7
		{"current version", 7, false, 0},
		{"future version", 8, false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updates, _, err := manager.GetUpdatesSince(tt.sinceVersion)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, updates, tt.expectedUpdates)
				if len(updates) > 0 {
					assert.Equal(t, tt.sinceVersion+1, updates[0].Version)
				}
			}
		})
	}
}

func TestClusterStateHistoryManager_CircularBuffer(t *testing.T) {
	historySize := 10
	manager := newTestHistoryManager(historySize)

	// Add 15 nodes, so history wraps around
	for i := 1; i <= 15; i++ {
		manager.AddNode(&common.NodeInfo{ID: fmt.Sprintf("node%d", i)})
	}

	assert.Equal(t, int64(15), manager.GetCurrentVersion())

	// Oldest version should be 15 - 10 + 1 = 6
	oldestVersion := manager.GetOldestVersionInHistory()
	assert.Equal(t, int64(6), oldestVersion)

	// Check IsVersionTooOld
	assert.True(t, manager.IsVersionTooOld(5))
	assert.False(t, manager.IsVersionTooOld(6))

	// GetUpdatesSince for a version that's been purged should fail
	_, _, err := manager.GetUpdatesSince(5)
	assert.Error(t, err)

	// GetUpdatesSince from just before the oldest should return full history
	updates, _, err := manager.GetUpdatesSince(6)
	assert.NoError(t, err)
	assert.Len(t, updates, 9) // 7, 8, 9, 10, 11, 12, 13, 14, 15
	if len(updates) > 0 {
		assert.Equal(t, int64(7), updates[0].Version)
		assert.Equal(t, int64(15), updates[len(updates)-1].Version)
	}

	// Add one more node
	manager.AddNode(&common.NodeInfo{ID: "node16"}) // Version 16
	assert.Equal(t, int64(7), manager.GetOldestVersionInHistory())
	assert.True(t, manager.IsVersionTooOld(6))
}

func TestClusterStateHistoryManager_ApplyUpdates(t *testing.T) {
	manager := newTestHistoryManager(10)
	updates := []common.NodeUpdate{
		{Version: 1, Type: common.NODE_ADDED, Node: &common.NodeInfo{ID: "node1"}},
		{Version: 2, Type: common.NODE_ADDED, Node: &common.NodeInfo{ID: "node2"}},
		{Version: 3, Type: common.NODE_REMOVED, Node: &common.NodeInfo{ID: "node1"}},
	}

	manager.ApplyUpdates(updates)

	nodes, version := manager.ListNodes()
	assert.Equal(t, int64(3), version)
	assert.Len(t, nodes, 1)
	assert.Equal(t, "node2", nodes[0].ID)

	_, ok := manager.GetNode("node1")
	assert.False(t, ok)
}

func TestClusterStateHistoryManager_InitializeNodes(t *testing.T) {
	manager := newTestHistoryManager(10)
	nodes := []*common.NodeInfo{
		{ID: "node1"},
		{ID: "node2"},
	}
	manager.InitializeNodes(nodes, 25)

	gotNodes, version := manager.ListNodes()
	assert.Equal(t, int64(25), version)
	assert.Len(t, gotNodes, 2)
}

func TestClusterStateHistoryManager_GetAvailableNodesForChunk(t *testing.T) {
	manager := newTestHistoryManager(10)
	n1 := &common.NodeInfo{ID: "node1", Status: common.NodeHealthy}
	n2 := &common.NodeInfo{ID: "node2", Status: common.NodeUnhealthy}
	n3 := &common.NodeInfo{ID: "node3", Status: common.NodeHealthy}

	manager.InitializeNodes([]*common.NodeInfo{n1, n2, n3}, 3)

	t.Run("some nodes healthy", func(t *testing.T) {
		replicaIDs := []*common.NodeInfo{{ID: "node1"}, {ID: "node2"}, {ID: "node3"}}
		available, ok := manager.GetAvailableNodesForChunk(replicaIDs)
		assert.True(t, ok)
		assert.Len(t, available, 2)
		assert.ElementsMatch(t, []*common.NodeInfo{n1, n3}, available)
	})

	t.Run("no nodes healthy", func(t *testing.T) {
		replicaIDs := []*common.NodeInfo{{ID: "node2"}}
		_, ok := manager.GetAvailableNodesForChunk(replicaIDs)
		assert.False(t, ok)
	})

	t.Run("node not in cluster", func(t *testing.T) {
		replicaIDs := []*common.NodeInfo{{ID: "node4"}}
		_, ok := manager.GetAvailableNodesForChunk(replicaIDs)
		assert.False(t, ok)
	})
}

func TestClusterStateHistoryManager_Concurrency(t *testing.T) {
	historySize := 200
	manager := newTestHistoryManager(historySize)
	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrent additions
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			manager.AddNode(&common.NodeInfo{ID: fmt.Sprintf("node-%d", i)})
		}(i)
	}
	wg.Wait()

	nodes, version := manager.ListNodes()
	assert.Equal(t, numGoroutines, len(nodes))
	assert.Equal(t, int64(numGoroutines), version)

	// Concurrent updates and removals
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				assert.NoError(t, manager.RemoveNode(fmt.Sprintf("node-%d", i)))
			} else {
				node := &common.NodeInfo{ID: fmt.Sprintf("node-%d", i), Host: "updated"}
				assert.NoError(t, manager.UpdateNode(node))
			}
		}(i)
	}
	wg.Wait()

	nodes, version = manager.ListNodes()
	assert.Equal(t, numGoroutines/2, len(nodes))
	assert.Equal(t, int64(numGoroutines*2), version)

	// Verify final state
	for _, node := range nodes {
		assert.Equal(t, "updated", node.Host)
	}

	// Verify history - oldest version is 1 (200 - 200 + 1)
	updates, _, err := manager.GetUpdatesSince(0)
	assert.Error(t, err, "requesting version 0 should fail as it's too old")
	assert.Nil(t, updates)

	updates, _, err = manager.GetUpdatesSince(1)
	assert.NoError(t, err)
	assert.Len(t, updates, 199, "should get updates from version 2 to 200")

	// Add 50 more nodes to cause history to wrap. Version will go to 250.
	for i := 0; i < 50; i++ {
		manager.AddNode(&common.NodeInfo{ID: fmt.Sprintf("new-node-%d", i)})
	}
	assert.Equal(t, int64(250), manager.GetCurrentVersion())
	// Oldest version is now 250 - 200 + 1 = 51
	assert.Equal(t, int64(51), manager.GetOldestVersionInHistory())

	// Concurrently get updates
	var errorCount atomic.Int32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch i % 4 {
			case 0:
				// These should fail (version too old)
				_, _, err := manager.GetUpdatesSince(50)
				if err == nil {
					errorCount.Add(1)
					t.Log("expected an error for too old version, but got none")
				}
			case 1:
				// These should succeed
				updates, _, err := manager.GetUpdatesSince(200)
				if err != nil || len(updates) != 50 {
					errorCount.Add(1)
					t.Logf("expected 50 updates, got %d, err: %v", len(updates), err)
				}
			default:
				// Just read
				manager.ListNodes()
			}
		}(i)
	}
	wg.Wait()

	assert.Zero(t, errorCount.Load(), "all concurrent update checks should pass")
}
