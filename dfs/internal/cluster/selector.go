package cluster

import (
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
)

type NodeSelector interface {
	SelectBestNodes(n int) ([]*common.DataNodeInfo, bool)
}

type nodeSelector struct {
	clusterViewer state.ClusterStateViewer
}

func NewNodeSelector(clusterViewer state.ClusterStateViewer) *nodeSelector {
	return &nodeSelector{
		clusterViewer: clusterViewer,
	}
}

func (s *nodeSelector) SelectBestNodes(n int) ([]*common.DataNodeInfo, bool) {
	var healthyNodes []*common.DataNodeInfo
	nodes, _ := s.clusterViewer.ListNodes()
	for _, node := range nodes {
		if node.Status == common.NodeHealthy {
			healthyNodes = append(healthyNodes, node)
		}
		if len(healthyNodes) >= n {
			break
		}
	}

	return healthyNodes, len(healthyNodes) >= n
}
