package cluster

import (
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
)

type NodeSelector interface {
	SelectBestNodes(n int, self *common.NodeInfo) ([]*common.NodeInfo, error)
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
func (s *nodeSelector) SelectBestNodes(n int, self *common.NodeInfo) ([]*common.NodeInfo, error) {
	if n <= 0 {
		return nil, ErrNoAvailableNodes
	}

	nodes, _ := s.clusterViewer.ListNodes()
	var selectedNodes []*common.NodeInfo
	for _, node := range nodes {
		// Stop when we have enough nodes
		if len(selectedNodes) >= n {
			break
		}
		if node.Status == common.NodeHealthy && node.ID != self.ID {
			selectedNodes = append(selectedNodes, node)
		}
	}

	if len(selectedNodes) == 0 {
		return nil, ErrNoAvailableNodes
	}

	return selectedNodes, nil
}
