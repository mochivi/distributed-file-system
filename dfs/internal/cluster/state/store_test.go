package state

import (
	"testing"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/assert"
)

func TestNewNodeStore(t *testing.T) {
	store := newNodeStore()
	assert.NotNil(t, store)
	assert.NotNil(t, store.nodes)
	assert.Empty(t, store.nodes)
}

func TestNodeStore_AddAndGetNode(t *testing.T) {
	store := newNodeStore()
	node1 := &common.NodeInfo{ID: "node1", Host: "host1"}

	// Add and get
	store.addNode(node1)
	gotNode, ok := store.getNode("node1")
	assert.True(t, ok)
	assert.Equal(t, node1, gotNode)

	// Get non-existent
	_, ok = store.getNode("non-existent")
	assert.False(t, ok)

	// Overwrite existing
	node1Updated := &common.NodeInfo{ID: "node1", Host: "host1-updated"}
	store.addNode(node1Updated)
	gotNode, ok = store.getNode("node1")
	assert.True(t, ok)
	assert.Equal(t, "host1-updated", gotNode.Host)
}

func TestNodeStore_RemoveNode(t *testing.T) {
	store := newNodeStore()
	node1 := &common.NodeInfo{ID: "node1"}
	store.addNode(node1)

	// Remove existing
	err := store.removeNode("node1")
	assert.NoError(t, err)
	_, ok := store.getNode("node1")
	assert.False(t, ok)

	// Remove non-existent
	err = store.removeNode("non-existent")
	assert.Error(t, err)
	assert.EqualError(t, err, "node with ID non-existent not found")
}

func TestNodeStore_UpdateNode(t *testing.T) {
	store := newNodeStore()
	node1 := &common.NodeInfo{ID: "node1", Host: "host1"}
	store.addNode(node1)

	// Update existing
	node1Updated := &common.NodeInfo{ID: "node1", Host: "host1-updated"}
	err := store.updateNode(node1Updated)
	assert.NoError(t, err)
	gotNode, ok := store.getNode("node1")
	assert.True(t, ok)
	assert.Equal(t, "host1-updated", gotNode.Host)

	// Update non-existent
	err = store.updateNode(&common.NodeInfo{ID: "non-existent"})
	assert.Error(t, err)
	assert.EqualError(t, err, "node with ID non-existent not found")
}

func TestNodeStore_ListNodes(t *testing.T) {
	store := newNodeStore()
	node1 := &common.NodeInfo{ID: "node1"}
	node2 := &common.NodeInfo{ID: "node2"}
	node3 := &common.NodeInfo{ID: "node3"}
	store.addNode(node1)
	store.addNode(node2)
	store.addNode(node3)

	tests := []struct {
		name          string
		limit         []int
		expectedCount int
	}{
		{"no limit", nil, 3},
		{"with limit", []int{2}, 2},
		{"limit of 0", []int{0}, 0},
		{"limit larger than size", []int{5}, 3},
		{"empty limit slice", []int{}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := store.listNodes(tt.limit...)
			assert.Len(t, nodes, tt.expectedCount)
		})
	}

	t.Run("empty store", func(t *testing.T) {
		emptyStore := newNodeStore()
		nodes := emptyStore.listNodes()
		assert.Empty(t, nodes)
		nodes = emptyStore.listNodes(5)
		assert.Empty(t, nodes)
	})
}

func TestNodeStore_InitializeNodes(t *testing.T) {
	store := newNodeStore()
	store.addNode(&common.NodeInfo{ID: "old-node"})

	newNodes := []*common.NodeInfo{
		{ID: "node1"},
		{ID: "node2"},
	}

	store.initializeNodes(newNodes)

	nodes := store.listNodes()
	assert.Len(t, nodes, 2)

	_, ok := store.getNode("old-node")
	assert.False(t, ok, "old node should be gone")

	gotNode1, ok := store.getNode("node1")
	assert.True(t, ok)
	assert.Equal(t, newNodes[0], gotNode1)

	gotNode2, ok := store.getNode("node2")
	assert.True(t, ok)
	assert.Equal(t, newNodes[1], gotNode2)

	// Initialize with empty slice
	store.initializeNodes([]*common.NodeInfo{})
	nodes = store.listNodes()
	assert.Empty(t, nodes)
}
