package state

import (
	"testing"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNodeSelector_SelectBestNodes(t *testing.T) {
	nodes := []*common.NodeInfo{
		{ID: "node-1", Status: common.NodeHealthy},
		{ID: "node-2", Status: common.NodeUnhealthy},
		{ID: "node-3", Status: common.NodeHealthy},
		{ID: "node-4", Status: common.NodeHealthy},
		{ID: "node-5", Status: common.NodeUnhealthy},
	}

	testCases := []struct {
		name          string
		n             int
		nodesToReturn []*common.NodeInfo
		expectedNodes []*common.NodeInfo
		expectedErr   error
	}{
		{
			name:          "success: select 2 healthy nodes",
			n:             2,
			nodesToReturn: nodes,
			expectedNodes: []*common.NodeInfo{
				{ID: "node-3", Status: common.NodeHealthy},
				{ID: "node-4", Status: common.NodeHealthy},
			},
			expectedErr: nil,
		},
		{
			name:          "success: select 3 healthy nodes except self",
			n:             3,
			nodesToReturn: nodes,
			expectedNodes: []*common.NodeInfo{
				{ID: "node-3", Status: common.NodeHealthy},
				{ID: "node-4", Status: common.NodeHealthy},
			},
			expectedErr: nil,
		},
		{
			name:          "success: request 6 nodes, but only 2 are healthy",
			n:             6,
			nodesToReturn: nodes,
			expectedNodes: []*common.NodeInfo{
				{ID: "node-3", Status: common.NodeHealthy},
				{ID: "node-4", Status: common.NodeHealthy},
			},
			expectedErr: nil,
		},
		{
			name:          "error: request 0 nodes",
			n:             0,
			nodesToReturn: nodes,
			expectedNodes: nil,
			expectedErr:   ErrNoAvailableNodes,
		},
		{
			name: "error: no healthy nodes available",
			n:    1,
			nodesToReturn: []*common.NodeInfo{
				{ID: "node-1", Status: common.NodeUnhealthy},
				{ID: "node-2", Status: common.NodeUnhealthy},
			},
			expectedNodes: nil,
			expectedErr:   ErrNoAvailableNodes,
		},
		{
			name:          "error: no nodes in cluster",
			n:             1,
			nodesToReturn: []*common.NodeInfo{},
			expectedNodes: nil,
			expectedErr:   ErrNoAvailableNodes,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockViewer := &MockClusterStateManager{}
			mockViewer.On("ListNodes", mock.Anything).Return(tc.nodesToReturn, int64(len(tc.nodesToReturn))).Maybe()

			selector := NewNodeSelector(mockViewer)
			selectedNodes, err := selector.SelectBestNodes(tc.n, &common.NodeInfo{ID: "node-1"})

			assert.Equal(t, tc.expectedErr, err)
			assert.ElementsMatch(t, tc.expectedNodes, selectedNodes)
			mockViewer.AssertExpectations(t)
		})
	}
}
