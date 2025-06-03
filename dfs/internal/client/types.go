package client

import (
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/internal/datanode"
)

// Client performs operations to reach coordinator or data nodes
type Client struct {
	datanodeClient    datanode.DataNodeClient
	coordinatorClient coordinator.CoordinatorClient
}
