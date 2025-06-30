package cluster_test

import (
	"testing"

	"github.com/mochivi/distributed-file-system/internal/cluster"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNodeSelector_SelectBestNodes(t *testing.T) {
	nodes := []*common.DataNodeInfo{
		{ID: "node-1", Status: common.NodeHealthy},
		{ID: "node-2", Status: common.NodeUnhealthy},
		{ID: "node-3", Status: common.NodeHealthy},
		{ID: "node-4", Status: common.NodeHealthy},
		{ID: "node-5", Status: common.NodeUnhealthy},
	}

	testCases := []struct {
		name          string
		n             int
		nodesToReturn []*common.DataNodeInfo
		expectedNodes []*common.DataNodeInfo
		expectedOk    bool
	}{
		{
			name:          "Select 2 healthy nodes",
			n:             2,
			nodesToReturn: nodes,
			expectedNodes: []*common.DataNodeInfo{
				{ID: "node-1", Status: common.NodeHealthy},
				{ID: "node-3", Status: common.NodeHealthy},
			},
			expectedOk: true,
		},
		{
			name:          "Select 3 healthy nodes",
			n:             3,
			nodesToReturn: nodes,
			expectedNodes: []*common.DataNodeInfo{
				{ID: "node-1", Status: common.NodeHealthy},
				{ID: "node-3", Status: common.NodeHealthy},
				{ID: "node-4", Status: common.NodeHealthy},
			},
			expectedOk: true,
		},
		{
			name:          "Request more healthy nodes than available",
			n:             4,
			nodesToReturn: nodes,
			expectedNodes: []*common.DataNodeInfo{
				{ID: "node-1", Status: common.NodeHealthy},
				{ID: "node-3", Status: common.NodeHealthy},
				{ID: "node-4", Status: common.NodeHealthy},
			},
			expectedOk: false,
		},
		{
			name:          "Request 0 nodes",
			n:             0,
			nodesToReturn: nodes,
			expectedNodes: []*common.DataNodeInfo{},
			expectedOk:    false,
		},
		{
			name: "No healthy nodes available",
			n:    1,
			nodesToReturn: []*common.DataNodeInfo{
				{ID: "node-1", Status: common.NodeUnhealthy},
				{ID: "node-2", Status: common.NodeUnhealthy},
			},
			expectedNodes: []*common.DataNodeInfo{},
			expectedOk:    false,
		},
		{
			name:          "No nodes in cluster",
			n:             1,
			nodesToReturn: []*common.DataNodeInfo{},
			expectedNodes: []*common.DataNodeInfo{},
			expectedOk:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockViewer := state.NewMockClusterStateManager()
			if tc.name != "Request 0 nodes" {
				mockViewer.On("ListNodes", mock.Anything).Return(tc.nodesToReturn, int64(len(tc.nodesToReturn)))
			}

			selector := cluster.NewNodeSelector(mockViewer)
			selectedNodes, ok := selector.SelectBestNodes(tc.n)

			assert.Equal(t, tc.expectedOk, ok)
			assert.ElementsMatch(t, tc.expectedNodes, selectedNodes)
			mockViewer.AssertExpectations(t)
		})
	}
}
