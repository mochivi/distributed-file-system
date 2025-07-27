package common

import (
	"fmt"
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
	Deleted    bool
	DeletedAt  time.Time
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
		Deleted:    pb.Deleted,
		DeletedAt:  pb.DeletedAt.AsTime(),
	}
}

func (fi FileInfo) ToProto() *proto.FileInfo {
	protoChunks := make([]*proto.ChunkInfo, 0, len(fi.Chunks))
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
		Deleted:    fi.Deleted,
		DeletedAt:  timestamppb.New(fi.DeletedAt),
	}
}

// ChunkHeader + proto conversions
type ChunkHeader struct {
	ID       string
	Version  byte
	Index    int // Index of the chunk in the file
	Size     int64
	Checksum string
}

func ChunkHeaderFromProto(pb *proto.ChunkHeader) ChunkHeader {
	return ChunkHeader{
		ID:       pb.Id,
		Version:  byte(pb.Version),
		Index:    int(pb.Index),
		Size:     pb.Size,
		Checksum: pb.Checksum,
	}
}

func (ch ChunkHeader) ToProto() *proto.ChunkHeader {
	return &proto.ChunkHeader{
		Id:       ch.ID,
		Version:  uint32(ch.Version),
		Index:    int32(ch.Index),
		Size:     ch.Size,
		Checksum: ch.Checksum,
	}
}

type ChunkInfo struct {
	Header   ChunkHeader
	Replicas []*NodeInfo // DataNode IDs storing this chunk
}

func ChunkInfoFromProto(pb *proto.ChunkInfo) ChunkInfo {
	replicas := make([]*NodeInfo, 0, len(pb.Replicas))
	for _, replica := range pb.Replicas {
		replicas = append(replicas, NodeInfoFromProto(replica))
	}
	return ChunkInfo{
		Header:   ChunkHeaderFromProto(pb.Header),
		Replicas: replicas,
	}
}

func (ci ChunkInfo) ToProto() *proto.ChunkInfo {
	protoReplicas := make([]*proto.NodeInfo, 0, len(ci.Replicas))
	for _, replica := range ci.Replicas {
		protoReplicas = append(protoReplicas, replica.ToProto())
	}
	return &proto.ChunkInfo{
		Header:   ci.Header.ToProto(),
		Replicas: protoReplicas,
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
