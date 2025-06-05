package datanode

import "github.com/mochivi/distributed-file-system/internal/common"

type nodeSelector struct {
}

func NewNodeSelector() *nodeSelector {
	return &nodeSelector{}
}

func (s *nodeSelector) selectBestNodes(n int) []common.DataNodeInfo {
	return []common.DataNodeInfo{}
}
