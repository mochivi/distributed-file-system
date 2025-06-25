package cluster

import (
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NodeUpdateType int

const (
	NODE_ADDED NodeUpdateType = iota
	NODE_REMOVED
	NODE_UPDATED
)

type NodeUpdate struct {
	Version   int64
	Type      NodeUpdateType
	Node      *common.DataNodeInfo
	Timestamp time.Time
}

func NodeUpdateFromProto(pb *proto.NodeUpdate) NodeUpdate {
	node := common.DataNodeInfoFromProto(pb.Node)
	return NodeUpdate{
		Version:   pb.Version,
		Type:      NodeUpdateType(pb.Type),
		Node:      &node,
		Timestamp: pb.Timestamp.AsTime(),
	}
}

func (nu NodeUpdate) ToProto() *proto.NodeUpdate {
	return &proto.NodeUpdate{
		Version:   nu.Version,
		Type:      proto.NodeUpdate_UpdateType(nu.Type),
		Node:      (*nu.Node).ToProto(),
		Timestamp: timestamppb.New(nu.Timestamp),
	}
}
