package coordinator

import (
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
)

// Request/response pairs

// Upload
type UploadRequest struct {
	Path      string // Path to upload the file
	Size      int    // File size in bytes
	ChunkSize int    // What chunkSize to break it down into
	Checksum  string // File sha256 checksum
}

func newUploadRequestFromProto(pb *proto.UploadRequest) UploadRequest {
	return UploadRequest{
		Path:      pb.Path,
		Size:      int(pb.Size),
		ChunkSize: int(pb.ChunkSize),
		Checksum:  pb.Checksum,
	}
}

type UploadResponse struct {
	ChunkLocations []ChunkLocation //
}

func (ur UploadResponse) ToProto() *proto.UploadResponse {
	protoChunkAssignments := make([]*proto.ChunkLocation, len(ur.ChunkLocations))
	for _, item := range ur.ChunkLocations {
		protoChunkAssignments = append(protoChunkAssignments, item.ToProto())
	}
	return &proto.UploadResponse{
		ChunkLocations: protoChunkAssignments,
	}
}

// Download
type DownloadRequest struct {
	Path string
}
type DownloadResponse struct {
	fileInfo       common.FileInfo
	chunkLocations []ChunkLocation
}

func (dr DownloadResponse) ToProto() *proto.DownloadResponse {
	protoChunkLocations := make([]*proto.ChunkLocation, len(dr.chunkLocations))
	for _, item := range dr.chunkLocations {
		protoChunkLocations = append(protoChunkLocations, item.ToProto())
	}
	return &proto.DownloadResponse{
		FileInfo:       dr.fileInfo.ToProto(),
		ChunkLocations: protoChunkLocations,
	}
}

// Delete
type DeleteRequest struct {
	Path string
}
type DeleteResponse struct {
	Success bool
	Message string
}

// List
type ListRequest struct {
	Directory string
}
type ListResponse struct {
	Files []common.FileInfo
}

// Data nodes registration
type RegisterDataNodeRequest struct {
	NodeInfo common.DataNodeInfo
}
type RegisterDataNodeResponse struct {
	Success bool
	Message string
}

func (r RegisterDataNodeResponse) ToProto() *proto.RegisterDataNodeResponse {
	return &proto.RegisterDataNodeResponse{
		Success: r.Success,
		Message: r.Message,
	}
}

// Heartbeat
type HeartbeatRequest struct {
	NodeID string
	Status common.HealthStatus
}

func HeartbeatRequestFromProto(pb *proto.HeartbeatRequest) HeartbeatRequest {
	return HeartbeatRequest{
		NodeID: pb.NodeId,
		Status: common.HealthStatusFromProto(pb.Status),
	}
}

type HeartbeatResponse struct {
	Success bool
}

func (hr HeartbeatResponse) ToProto() *proto.HeartbeatResponse {
	return &proto.HeartbeatResponse{
		Success: hr.Success,
	}
}
