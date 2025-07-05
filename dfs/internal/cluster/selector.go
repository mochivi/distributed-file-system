package cluster

import (
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
)

type NodeSelector interface {
	SelectBestNodes(n int) ([]*common.NodeInfo, bool)
}

type nodeSelector struct {
	clusterViewer state.ClusterStateViewer
}

func NewNodeSelector(clusterViewer state.ClusterStateViewer) *nodeSelector {
	return &nodeSelector{
		clusterViewer: clusterViewer,
	}
}

// TODO: configure for overlap between chunks for the same node in cases there are few available datanodes
func (s *nodeSelector) SelectBestNodes(n int) ([]*common.NodeInfo, bool) {
	if n <= 0 {
		return nil, false
	}

	nodes, _ := s.clusterViewer.ListNodes()
	var onlyHealthyNodes []*common.NodeInfo
	for _, node := range nodes {
		if node.Status == common.NodeHealthy {
			onlyHealthyNodes = append(onlyHealthyNodes, node)
		}
	}

	if len(onlyHealthyNodes) == 0 {
		return nil, false
	}

	var selectedNodes []*common.NodeInfo
	for i := 0; i < n; i++ {
		node := onlyHealthyNodes[i%len(onlyHealthyNodes)]
		selectedNodes = append(selectedNodes, node)
	}

	return selectedNodes, true
}
