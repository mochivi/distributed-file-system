package cluster

// import (
// 	"github.com/mochivi/distributed-file-system/internal/datanode"
// )

// type ClusterNode struct {
// 	server      *datanode.DataNodeServer
// 	nodeManager *NodeManager
// 	services    struct {
// 		heartbeat *HeartbeatService
// 		// register  *RegisterService
// 	}
// }

// type HeartbeatService struct {
// 	cluster *ClusterNode
// }

// func NewHeartbeatService(cluster *ClusterNode) *HeartbeatService {
// 	return &HeartbeatService{cluster: cluster}
// }

// func NewNode(server *datanode.DataNodeServer, nodeManager *NodeManager) *ClusterNode {
// 	return &ClusterNode{
// 		server:      server,
// 		nodeManager: nodeManager,
// 	}
// }

// func (c *ClusterNode) Run() error {
// 	return nil
// }
