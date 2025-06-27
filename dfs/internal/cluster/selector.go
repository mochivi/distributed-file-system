package cluster

import "github.com/mochivi/distributed-file-system/internal/common"

/*
TODO
*/

type NodeSelector interface {
	SelectBestNodes(nodes []*common.DataNodeInfo, n int) []*common.DataNodeInfo
}

type nodeSelector struct {
	nodeManager INodeManager
}

func NewNodeSelector(nodeManager INodeManager) *nodeSelector {
	return &nodeSelector{
		nodeManager: nodeManager,
	}
}

func (s *nodeSelector) SelectBestNodes(nodes []*common.DataNodeInfo, n int) []*common.DataNodeInfo {
	var healthyNodes []*common.DataNodeInfo
	for _, node := range nodes {
		if node.Status == common.NodeHealthy {
			healthyNodes = append(healthyNodes, node)
		}
		if len(healthyNodes) >= n {
			break
		}
	}

	return healthyNodes
}
