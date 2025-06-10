package common

type NodeSelector interface {
	SelectBestNodes(nodes []*DataNodeInfo, n int) []*DataNodeInfo
}

type nodeSelector struct {
}

func NewNodeSelector() *nodeSelector {
	return &nodeSelector{}
}

func (s *nodeSelector) SelectBestNodes(nodes []*DataNodeInfo, n int) []*DataNodeInfo {
	var healthyNodes []*DataNodeInfo
	for _, node := range nodes {
		if node.Status == NodeHealthy {
			healthyNodes = append(healthyNodes, node)
		}
		if len(healthyNodes) >= n {
			break
		}
	}

	return healthyNodes
}
