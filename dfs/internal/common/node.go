package common

import (
	"fmt"
	"time"

	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	NodeHealthy NodeStatus = iota
	NodeDegraded
	NodeUnhealthy
)

type NodeUpdateType int

const (
	NODE_ADDED NodeUpdateType = iota
	NODE_REMOVED
	NODE_UPDATED
)

type NodeStatus int

type HealthStatus struct {
	Status   NodeStatus
	LastSeen time.Time
}

func HealthStatusFromProto(pb *proto.HealthStatus) HealthStatus {
	return HealthStatus{
		Status:   NodeStatus(pb.Status),
		LastSeen: pb.LastSeen.AsTime(),
	}
}

func (hs HealthStatus) ToProto() *proto.HealthStatus {
	return &proto.HealthStatus{
		Status:   proto.NodeStatus(hs.Status),
		LastSeen: timestamppb.New(hs.LastSeen),
	}
}

// NodeInfo + proto conversions
type NodeInfo struct {
	ID       string
	Host     string
	Port     int
	Capacity int // Total storage capacity in bytes
	Used     int // Currently used storage in bytes
	Status   NodeStatus
	LastSeen time.Time
}

func (di NodeInfo) String() string {
	return fmt.Sprintf("NodeInfo{ID: %s, Host: %s, Port: %d, Status: %v}", di.ID, di.Host, di.Port, di.Status)
}

func (di NodeInfo) Endpoint() string {
	return fmt.Sprintf("%s:%d", di.Host, di.Port)
}

func NodeInfoFromProto(pb *proto.NodeInfo) *NodeInfo {
	return &NodeInfo{
		ID:       pb.Id,
		Host:     pb.IpAddress,
		Port:     int(pb.Port),
		Capacity: int(pb.Capacity),
		Used:     int(pb.Used),
		Status:   NodeStatus(pb.Status),
		LastSeen: pb.LastSeen.AsTime(),
	}
}

func (di NodeInfo) ToProto() *proto.NodeInfo {
	timestamp := timestamppb.New(di.LastSeen)
	if di.LastSeen.IsZero() {
		timestamp = nil
	}

	return &proto.NodeInfo{
		Id:        di.ID,
		IpAddress: di.Host,
		Port:      int32(di.Port),
		Capacity:  int64(di.Capacity),
		Used:      int64(di.Used),
		Status:    proto.NodeStatus(di.Status),
		LastSeen:  timestamp,
	}
}
