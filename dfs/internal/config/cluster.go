package config

import (
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
)

type ClusterNodeRole string

const (
	ClusterNodeRoleCoordinator ClusterNodeRole = "coordinator"
	ClusterNodeRoleDataNode    ClusterNodeRole = "datanode"
)

// Control loops configuration
type ClusterNodeConfig struct {
	Role      ClusterNodeRole
	Node      *common.DataNodeInfo // Only the gRPC server information
	Heartbeat *HeartbeatControllerConfig
}

func DefaultClusterNodeConfig() *ClusterNodeConfig {
	return &ClusterNodeConfig{
		Role:      ClusterNodeRoleDataNode,
		Node:      nil,
		Heartbeat: DefaultHeartbeatControllerConfig(),
	}
}

type HeartbeatControllerConfig struct {
	Interval time.Duration // how often to send heartbeats to the coordinator
	Timeout  time.Duration // how long to wait for a heartbeat response from the coordinator
}

func DefaultHeartbeatControllerConfig() *HeartbeatControllerConfig {
	return &HeartbeatControllerConfig{
		Interval: 30 * time.Second,
		Timeout:  10 * time.Second,
	}
}

type NodeManagerConfig struct {
	CoordinatorNodeManagerConfig CoordinatorNodeManagerConfig
	DataNodeManagerConfig        DataNodeManagerConfig
}

type CoordinatorNodeManagerConfig struct {
}

type DataNodeManagerConfig struct {
	MaxHistorySize int // max number of updates to keep in history
}

func DefaultDataNodeManagerConfig() DataNodeManagerConfig {
	return DataNodeManagerConfig{
		MaxHistorySize: 1000,
	}
}

func DefaultNodeManagerConfig() NodeManagerConfig {
	return NodeManagerConfig{
		CoordinatorNodeManagerConfig: CoordinatorNodeManagerConfig{},
		DataNodeManagerConfig:        DefaultDataNodeManagerConfig(),
	}
}
