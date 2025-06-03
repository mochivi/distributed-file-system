package common

import (
	"time"

	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Data types and constants
type NodeStatus int

const (
	NodeHealthy NodeStatus = iota
	NodeDegraded
	NodeUnhealthy
)

// Data structures
// FileInfo + proto conversions
type FileInfo struct {
	Path       string
	Size       int
	ChunkCount int
	Chunks     []ChunkInfo
	CreatedAt  time.Time
	Checksum   string
}

func FileInfoFromProto(pb *proto.FileInfo) FileInfo {
	chunks := make([]ChunkInfo, 0, len(pb.Chunks))
	for _, chunk := range pb.Chunks {
		chunks = append(chunks, ChunkInfoFromProto(chunk))
	}
	return FileInfo{
		Path:       pb.Path,
		Size:       int(pb.Size),
		ChunkCount: int(pb.ChunkCount),
		Chunks:     chunks,
		CreatedAt:  pb.CreatedAt.AsTime(),
		Checksum:   pb.Checksum,
	}
}

func (fi FileInfo) ToProto() *proto.FileInfo {
	protoChunks := make([]*proto.ChunkInfo, len(fi.Chunks))
	for _, item := range fi.Chunks {
		protoChunks = append(protoChunks, item.ToProto())
	}

	return &proto.FileInfo{
		Path:       fi.Path,
		Size:       int64(fi.Size),
		ChunkCount: int32(fi.ChunkCount),
		Chunks:     protoChunks,
		CreatedAt:  timestamppb.New(fi.CreatedAt),
		Checksum:   fi.Checksum,
	}
}

// ChunkInfo + proto conversions
type ChunkInfo struct {
	ID       string
	Size     int
	Replicas []string // DataNode IDs storing this chunk
	Checksum string
}

func ChunkInfoFromProto(pb *proto.ChunkInfo) ChunkInfo {
	return ChunkInfo{
		ID:       pb.Id,
		Size:     int(pb.Size),
		Replicas: pb.Replicas,
		Checksum: pb.Checksum,
	}
}

func (ci ChunkInfo) ToProto() *proto.ChunkInfo {
	return &proto.ChunkInfo{
		Id:       ci.ID,
		Size:     int64(ci.Size),
		Replicas: ci.Replicas,
		Checksum: ci.Checksum,
	}
}

// HealthStatus + proto conversions
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

// DataNodeInfo + proto conversions
type DataNodeInfo struct {
	ID        string
	IPAddress string
	Port      int
	Capacity  int // Total storage capacity in bytes
	Used      int // Currently used storage in bytes
	Status    NodeStatus
	LastSeen  time.Time
}

func DataNodeInfoFromProto(pb *proto.DataNodeInfo) DataNodeInfo {
	return DataNodeInfo{
		ID:        pb.Id,
		IPAddress: pb.IpAddress,
		Port:      int(pb.Port),
		Capacity:  int(pb.Capacity),
		Used:      int(pb.Used),
		Status:    NodeStatus(pb.Status),
		LastSeen:  pb.LastSeen.AsTime(),
	}
}

func (di DataNodeInfo) ToProto() *proto.DataNodeInfo {
	return &proto.DataNodeInfo{
		Id:        di.ID,
		IpAddress: di.IPAddress,
		Port:      int32(di.Port),
		Capacity:  int64(di.Capacity),
		Used:      int64(di.Used),
		Status:    proto.NodeStatus(di.Status),
		LastSeen:  timestamppb.New(di.LastSeen),
	}
}
