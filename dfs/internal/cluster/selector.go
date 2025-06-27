package cluster

import (
	"github.com/mochivi/distributed-file-system/internal/cluster/node_manager"
	"github.com/mochivi/distributed-file-system/internal/common"
)

type NodeSelector interface {
	SelectBestNodes(n int) ([]*common.DataNodeInfo, bool)
}

type nodeSelector struct {
	nodeManager node_manager.INodeManager
}

func NewNodeSelector(nodeManager node_manager.INodeManager) *nodeSelector {
	return &nodeSelector{
		nodeManager: nodeManager,
	}
}

func (s *nodeSelector) SelectBestNodes(n int) ([]*common.DataNodeInfo, bool) {
	var healthyNodes []*common.DataNodeInfo
	nodes, _ := s.nodeManager.ListNodes()
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
