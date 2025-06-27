package node_manager

import "github.com/mochivi/distributed-file-system/internal/common"

// INodeManager defines the interface for managing the cluster's node information from the
// perspective of a single node. It is designed to be used with dependency injection,
// allowing for flexible implementations, such as mocks for testing, which facilitates
// decoupled and testable components, for example, a mock could enforce writing to a specific node.
//
// An implementation of INodeManager, such as NodeManager, is a core component within a
// DataNode server. Its responsibilities include:
//
//  1. Node Registry: Maintaining a synchronized list of all known data nodes and
//     coordinator nodes in the cluster.
//
//  2. State Synchronization: Coordinating with the master Coordinator to keep the local node
//     list up-to-date. This is achieved through a versioning system (e.g., GetUpdatesSince,
//     ApplyHistory) that allows for efficient, incremental updates rather than fetching the
//     entire node list repeatedly.
//
//  3. Coordinator Leader Discovery: Identifying and providing access to the current leader of
//     the coordinator cluster (e.g., GetCoordinatorNode). This is crucial for DataNodes, which
//     send write requests like heartbeats and block reports to the leader. Caching the leader's
//     identity within the NodeManager makes these interactions more efficient.
//
//  4. Node Selection: Providing logic to select appropriate nodes for various tasks, such as
//     selecting datanodes for chunk replication (e.g., SelectBestNodes).
type INodeManager interface {

	// Node manager methods
	GetNode(nodeID string) (*common.DataNodeInfo, bool)
	AddNode(node *common.DataNodeInfo)
	RemoveNode(nodeID string) error
	UpdateNode(node *common.DataNodeInfo) error
	ListNodes() ([]*common.DataNodeInfo, int64)

	// Version control methods
	GetCurrentVersion() int64
	GetUpdatesSince(sinceVersion int64) ([]common.NodeUpdate, int64, error)
	IsVersionTooOld(version int64) bool

	// Write-only version control methods used by the clusterNode
	ApplyHistory(updates []common.NodeUpdate)                           // Heartbeat controller
	InitializeNodes(nodes []*common.DataNodeInfo, currentVersion int64) // Register service

	// Read-only methods used by the datanode
	// SelectBestNodes(n int, self ...string) ([]*common.DataNodeInfo, error)
	// GetAvailableNodesForChunk(replicaIDs []*common.DataNodeInfo) ([]*common.DataNodeInfo, bool)

	// Coordinator node manager methods
	GetCoordinatorNode() (*common.DataNodeInfo, bool)
	AddCoordinatorNode(node *common.DataNodeInfo)
	RemoveCoordinatorNode(nodeID string)
	ListCoordinatorNodes() []*common.DataNodeInfo
	GetLeaderCoordinatorNode() (*common.DataNodeInfo, bool)
	BootstrapCoordinatorNode() error
}

// Read-only methods used by the datanode
type IReadOnlyNodeManager interface {
	GetNode(nodeID string) (*common.DataNodeInfo, bool)
	GetLeaderCoordinatorNode() (*common.DataNodeInfo, bool)
	ListNodes() ([]*common.DataNodeInfo, int64)
	GetCoordinatorNode() (*common.DataNodeInfo, bool)
	ListCoordinatorNodes() []*common.DataNodeInfo
}

type NodeManager struct {
	coordinatorNodeManager *coordinatorNodeManager
	nodeManager            *datanodeManager
}

func NewNodeManager(config NodeManagerConfig) *NodeManager {
	return &NodeManager{
		coordinatorNodeManager: newCoordinatorNodeManager(config.CoordinatorNodeManagerConfig),
		nodeManager:            newDatanodeManager(config.DataNodeManagerConfig),
	}
}

// Exposes high level functions to the cluster node
func (m *NodeManager) GetNode(nodeID string) (*common.DataNodeInfo, bool) {
	return m.nodeManager.getNode(nodeID)
}

func (m *NodeManager) AddNode(node *common.DataNodeInfo) {
	m.nodeManager.addNode(node)
}

func (m *NodeManager) RemoveNode(nodeID string) error {
	return m.nodeManager.removeNode(nodeID)
}

func (m *NodeManager) UpdateNode(node *common.DataNodeInfo) error {
	return m.nodeManager.updateNode(node)
}

func (m *NodeManager) ListNodes() ([]*common.DataNodeInfo, int64) {
	return m.nodeManager.listNodes()
}

func (m *NodeManager) GetCurrentVersion() int64 {
	return m.nodeManager.getCurrentVersion()
}

func (m *NodeManager) GetUpdatesSince(sinceVersion int64) ([]common.NodeUpdate, int64, error) {
	return m.nodeManager.getUpdatesSince(sinceVersion)
}

func (m *NodeManager) IsVersionTooOld(version int64) bool {
	return m.nodeManager.isVersionTooOld(version)
}

func (m *NodeManager) ApplyHistory(updates []common.NodeUpdate) {
	m.nodeManager.applyHistory(updates)
}

func (m *NodeManager) InitializeNodes(nodes []*common.DataNodeInfo, currentVersion int64) {
	m.nodeManager.initializeNodes(nodes, currentVersion)
}

func (m *NodeManager) GetCoordinatorNode(nodeID string) (*common.DataNodeInfo, bool) {
	return m.coordinatorNodeManager.getCoordinatorNode(nodeID)
}

func (m *NodeManager) AddCoordinatorNode(node *common.DataNodeInfo) {
	m.coordinatorNodeManager.addCoordinatorNode(node)
}

func (m *NodeManager) RemoveCoordinatorNode(nodeID string) {
	m.coordinatorNodeManager.removeCoordinatorNode(nodeID)
}

func (m *NodeManager) ListCoordinatorNodes() []*common.DataNodeInfo {
	return m.coordinatorNodeManager.listCoordinatorNodes()
}

func (m *NodeManager) GetLeaderCoordinatorNode() (*common.DataNodeInfo, bool) {
	return m.coordinatorNodeManager.getLeaderCoordinatorNode()
}

func (m *NodeManager) BootstrapCoordinatorNode() error {
	return m.coordinatorNodeManager.bootstrapCoordinatorNode()
}
