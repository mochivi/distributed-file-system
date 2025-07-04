package state

import (
	"fmt"

	"github.com/mochivi/distributed-file-system/internal/common"
)

type nodeStore struct {
	nodes map[string]*common.NodeInfo
}

func newNodeStore() *nodeStore {
	return &nodeStore{
		nodes: make(map[string]*common.NodeInfo),
	}
}

func (m *nodeStore) getNode(nodeID string) (*common.NodeInfo, bool) {
	node, ok := m.nodes[nodeID]
	if !ok {
		return nil, false
	}
	return node, true
}

func (m *nodeStore) addNode(node *common.NodeInfo) {
	m.nodes[node.ID] = node
}

func (m *nodeStore) removeNode(nodeID string) error {
	_, ok := m.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}
	delete(m.nodes, nodeID)
	return nil
}

func (m *nodeStore) updateNode(node *common.NodeInfo) error {
	_, ok := m.nodes[node.ID]
	if !ok {
		return fmt.Errorf("node with ID %s not found", node.ID)
	}
	m.nodes[node.ID] = node
	return nil
}

func (m *nodeStore) listNodes(n ...int) []*common.NodeInfo {
	nodes := make([]*common.NodeInfo, 0, len(m.nodes))
	for _, node := range m.nodes {
		if len(n) > 0 && len(nodes) >= n[0] {
			break
		}
		nodes = append(nodes, node)
	}
	return nodes
}

func (m *nodeStore) initializeNodes(nodes []*common.NodeInfo) {
	m.nodes = make(map[string]*common.NodeInfo)
	for _, node := range nodes {
		m.nodes[node.ID] = node
	}
}
