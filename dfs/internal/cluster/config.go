package cluster

import "github.com/mochivi/distributed-file-system/internal/common"

type clusterNodeRole string

const (
	ClusterNodeRoleCoordinator clusterNodeRole = "coordinator"
	ClusterNodeRoleDataNode    clusterNodeRole = "datanode"
)

// Control loops configuration
type ClusterNodeConfig struct {
	role      clusterNodeRole
	node      *common.DataNodeInfo // Only the gRPC server information
	heartbeat *HeartbeatControllerConfig
}
